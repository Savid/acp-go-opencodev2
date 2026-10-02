package opencodeacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

// imageArtifact preserves admitted local output independently of the native path.
type imageArtifact struct {
	Data      string `json:"data"`
	MIME      string `json:"mime"`
	Refusal   string `json:"refusal,omitempty"`
	Reference string `json:"reference"`
}

// keepArtifact retains the bytes or refusal needed to replay native history.
func (s *session) keepArtifact(id string, artifact imageArtifact) {
	if s.artifacts == nil {
		s.artifacts = map[string]imageArtifact{}
	}

	s.artifacts[id] = artifact
}

func cloneArtifacts(values map[string]imageArtifact) map[string]imageArtifact {
	result := make(map[string]imageArtifact, len(values))
	maps.Copy(result, values)

	return result
}

func (s *session) imageBytes(file opencode.NativeAttachment) ([]byte, string, *image.OutputError) {
	limit := s.agent.options.ImageLimits.core().EffectiveOutputPerImage()
	s.mu.Lock()
	cached, ok := s.artifacts[file.ID]
	s.mu.Unlock()

	if ok && cached.Reference == imageReference(file.URL) {
		if cached.Refusal != "" {
			return nil, "", &image.OutputError{Reason: cached.Refusal}
		}

		data, mime, _, err := image.DecodeInline(cached.Data, limit)

		return data, mime, err
	}

	var (
		data    []byte
		mime    string
		refusal *image.OutputError
	)

	if strings.HasPrefix(file.URL, "data:") {
		prefix, payload, ok := strings.Cut(file.URL, ",")
		if !ok || !strings.HasSuffix(prefix, ";base64") {
			return nil, "", &image.OutputError{Reason: image.ReasonInvalidBase64}
		}

		data, mime, _, refusal = image.DecodeInline(payload, limit)
	} else {
		path := file.URL
		if parsed, err := url.Parse(path); err == nil && parsed.Scheme == partFile {
			if parsed.Host != "" && parsed.Host != "localhost" {
				return nil, "", &image.OutputError{Reason: image.ReasonPathNotAllowed}
			}

			path = parsed.Path
		}

		if !filepath.IsAbs(path) {
			return nil, "", &image.OutputError{Reason: image.ReasonPathNotAllowed}
		}

		roots := append([]string{s.cwd, os.TempDir()}, s.additionalDirectories...)
		if s.agent.options.ScratchDir != "" {
			roots = append(roots, s.agent.options.ScratchDir)
		}

		data, mime, refusal = image.ReadFile(path, roots, limit)
	}

	if refusal != nil {
		return nil, "", refusal
	}

	declared := strings.ToLower(strings.TrimSpace(file.MIME))
	switch declared {
	case image.MIMEPNG, image.MIMEJPEG, image.MIMEGIF, image.MIMEWebP, "image/bmp", "image/x-icon", "image/tiff":
		if declared != mime {
			return nil, "", &image.OutputError{Reason: image.ReasonMediaTypeMismatch}
		}
	}

	if storableArtifact(file) {
		s.mu.Lock()
		s.keepArtifact(file.ID, imageArtifact{Data: base64.StdEncoding.EncodeToString(data), MIME: mime, Reference: imageReference(file.URL)})
		s.mu.Unlock()
	}

	return data, mime, nil
}

func (s *session) outputFile(file opencode.NativeAttachment, used *int64) []acp.ContentBlock {
	if file.Data != "" {
		file.URL = "data:" + file.MIME + ";base64," + file.Data
	}

	if parsed, err := url.Parse(file.URL); err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") {
		return []acp.ContentBlock{{ResourceLink: &acp.ContentBlockResourceLink{Uri: file.URL, Name: file.Filename}}}
	}

	if !image.IsImageMIME(file.MIME) {
		return nil
	}

	data, mime, refusal := s.imageBytes(file)
	if refusal == nil && used != nil {
		*used += int64(len(data))
		if *used > s.agent.options.ImageLimits.core().EffectiveOutputPerToolCall() {
			refusal = &image.OutputError{Reason: image.ReasonTooLarge}
		}
	}

	if refusal != nil {
		if storableArtifact(file) {
			s.mu.Lock()
			s.keepArtifact(file.ID, imageArtifact{Refusal: refusal.Reason, Reference: imageReference(file.URL)})
			s.mu.Unlock()
		}

		guidance, _ := refusal.Guidance()

		return []acp.ContentBlock{acp.TextBlock(guidance)}
	}

	return []acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(data), mime)}
}

// captureImages admits every image the mirror cannot replay from its own rows
// through the output gate, which stores the sidecar copy a later replay reads.
// Only a storable artifact is decoded: a data URL carries its bytes in the
// mirrored row, so rastering it here would cost a decode on every commit and
// store nothing.
func (s *session) captureImages(rows [][]byte) {
	_ = walkImages(rows, s.nativeID, func(file opencode.NativeAttachment, used *int64) error {
		if image.IsImageMIME(file.MIME) && storableArtifact(file) {
			_ = s.outputFile(file, used)
		}

		return nil
	})
}
func walkImages(rows [][]byte, id string, visit func(opencode.NativeAttachment, *int64) error) error {
	messages := nativeMessages(rows, id)
	for i := range messages {
		message := &messages[i]
		for index, file := range message.Files {
			file.ID = attachmentID(message.ID, index)
			if err := visit(file, nil); err != nil {
				return err
			}
		}

		for index, part := range message.Content {
			if part.Type == partFile {
				if err := visit(opencode.NativeAttachment{ID: attachmentID(message.ID, index), MIME: part.MIME, URL: part.URI}, nil); err != nil {
					return err
				}
			}

			if part.Type == partTool {
				var state opencode.ToolState
				if err := json.Unmarshal(part.State, &state); err != nil {
					return err
				}

				used := int64(0)

				for n, item := range state.Content {
					if item.Type == partFile {
						if err := visit(opencode.NativeAttachment{ID: attachmentID(part.ID, n), MIME: item.MIME, URL: item.URI}, &used); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

// storableArtifact reports whether an admitted image needs its own sidecar
// copy. A data URL already carries its bytes in the mirrored native row, so a
// second copy in the configuration record would be written on every commit.
func storableArtifact(file opencode.NativeAttachment) bool {
	return file.ID != "" && !strings.HasPrefix(file.URL, "data:")
}

func attachmentID(part string, index int) string { return part + "/" + strconv.Itoa(index) }

func imageReference(value string) string {
	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])
}

func validateStoredImages(rows [][]byte, record sessionRecord) error {
	check := func(file opencode.NativeAttachment) error {
		if !image.IsImageMIME(file.MIME) || file.Data != "" || strings.HasPrefix(file.URL, "data:") {
			return nil
		}

		if parsed, err := url.Parse(file.URL); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
			return nil
		}

		cached, ok := record.Artifacts[file.ID]
		if !ok || cached.Reference != imageReference(file.URL) {
			return errors.New("stored local image artifact missing")
		}

		return nil
	}

	return walkImages(rows, record.NativeSessionID, func(file opencode.NativeAttachment, _ *int64) error { return check(file) })
}
