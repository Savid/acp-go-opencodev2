package opencodeacp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	acp "github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

func TestPromptImageGates(t *testing.T) {
	t.Parallel()

	raster := promptRaster(t)

	cases := []struct {
		name   string
		option Option
		block  acp.ContentBlock
		code   string
		field  string
	}{
		{"too large", WithImageLimits(ImageLimits{MaxInputBytesPerImage: 1}), acp.ImageBlock(raster, image.MIMEPNG), image.ErrorTooLarge, image.FieldPromptImage},
		{"invalid base64", nil, acp.ImageBlock("not base64", image.MIMEPNG), image.ErrorInvalidBase64, image.FieldPromptImage},
		{"media type mismatch", nil, acp.ImageBlock(raster, image.MIMEGIF), image.ErrorMediaTypeMismatch, image.FieldPromptImage},
		{"invalid media type", nil, acp.ImageBlock(raster, "image/bmp"), image.ErrorInvalidMediaType, image.FieldPromptImage},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options := []Option{}
			if tc.option != nil {
				options = append(options, tc.option)
			}

			h := newHarness(t, options...)
			h.initialize()
			session := h.newSession()

			_, err := h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, tc.block))
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, tc.code, data[wire.FieldError])
			require.Equal(t, tc.field, data["field"])
		})
	}
}

func TestPromptImageRefusedByTextOnlyModel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir(),
		WithSessionOpenCodeOptions(NewOpenCodeOptions(WithOpenCodeModel("fake/text")))))
	require.NoError(t, err)

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.ImageBlock(promptRaster(t), image.MIMEPNG)))
	require.Equal(t, image.ErrorUnsupportedByModel, requestErrorData(t, err)[wire.FieldError])
}

// handoffBlock builds an image block in the handoff form. An empty digest is
// derived from the file so a caller only states one to force a mismatch.
func handoffBlock(t *testing.T, path string, digest string, size int64) acp.ContentBlock {
	t.Helper()

	if digest == "" {
		data, err := os.ReadFile(path)
		if err == nil {
			sum := sha256.Sum256(data)
			digest = hex.EncodeToString(sum[:])
			size = int64(len(data))
		} else {
			digest = hex.EncodeToString(make([]byte, sha256.Size))
		}
	}

	uri := "file://" + path

	return acp.ContentBlock{Image: &acp.ContentBlockImage{
		Type:     "image",
		MimeType: image.MIMEPNG,
		Uri:      &uri,
		Meta: map[string]any{wire.HandoffKey: map[string]any{
			"version": 1, "digest": digest, "sizeBytes": size,
		}},
	}}
}

func TestPromptHandoffReadsAndVerifies(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "input.png")
	decoded, err := base64.StdEncoding.DecodeString(promptRaster(t))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, decoded, 0o600))

	// The pinned SDK drops a content block's _meta when it encodes a prompt, so
	// the handoff envelope reaches the gates only through the embedded Go API.
	prompt := func(t *testing.T, options []Option, block acp.ContentBlock) error {
		t.Helper()

		a := NewAgent(testOptions(t, options...)...)
		t.Cleanup(func() { _ = a.Close() })

		_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
		require.NoError(t, err)

		session, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
		require.NoError(t, err)

		_, err = a.Prompt(t.Context(), wire.PromptRequest(session.SessionId, acp.TextBlock("look"), block))

		return err
	}

	require.NoError(t, prompt(t, []Option{WithInputHandoffRoot(root)}, handoffBlock(t, path, "", 0)))

	mismatch := prompt(t, []Option{WithInputHandoffRoot(root)},
		handoffBlock(t, path, hex.EncodeToString(make([]byte, sha256.Size)), int64(len(decoded))))
	require.Equal(t, image.ErrorDigestMismatch, requestErrorData(t, mismatch)[wire.FieldError])

	unset := prompt(t, nil, handoffBlock(t, path, "", 0))
	require.Equal(t, image.ErrorInvalidHandoff, requestErrorData(t, unset)[wire.FieldError])
}

func TestPromptImagesReplayAndAreNotStoredTwice(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	raster := promptRaster(t)

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.TextBlock("look"), acp.ImageBlock(raster, image.MIMEPNG)))
	require.NoError(t, err)

	require.Empty(t, storedRecord(t, store, session.SessionId).Artifacts,
		"a prompt image already rides the mirrored native row")

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	restored := newHarness(t, WithSessionStore(store))
	restored.initialize()

	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)

	images := 0

	for _, notification := range restored.rec.snapshot() {
		if chunk := notification.Update.UserMessageChunk; chunk != nil && chunk.Content.Image != nil {
			images++

			require.Equal(t, raster, chunk.Content.Image.Data)
		}
	}

	require.Equal(t, 1, images, "load replays the user's image, not only the text")
}

func TestLocalImagesSurviveFullHistoryRestore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "image.png")
	raster := promptRaster(t)
	data, err := base64.StdEncoding.DecodeString(raster)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	store := acpcore.NewInMemorySessionStore()
	agent := NewAgent(WithSessionStore(store))
	t.Cleanup(func() { _ = agent.Close() })
	s := &session{agent: agent, id: "ses_images", nativeID: "ses_images", cwd: dir}
	content := make([]opencode.Content, 40)
	for i := range content {
		content[i] = opencode.Content{Type: partFile, URI: path, MIME: image.MIMEPNG}
	}
	row, err := json.Marshal(fakeExport{
		Info:     opencode.NativeSession{ID: s.nativeID, Location: opencode.Location{Directory: dir}},
		Messages: []opencode.NativeMessage{{ID: "msg_images", Type: roleAssistant, Content: content}},
	})
	require.NoError(t, err)
	rows := [][]byte{row}
	s.captureImages(rows)
	require.NoError(t, sessionlog.Commit(t.Context(), store, string(s.id), rows, s.record()))
	require.NoError(t, os.Remove(path))
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(s.id, dir))
	require.NoError(t, err)
	images := 0
	for _, notification := range h.rec.snapshot() {
		if chunk := notification.Update.AgentMessageChunk; chunk != nil && chunk.Content.Image != nil {
			images++
			require.Equal(t, raster, chunk.Content.Image.Data)
		}
	}
	require.Equal(t, len(content), images)
}
