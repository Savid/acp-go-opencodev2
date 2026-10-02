package opencode

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const InternalEnvPrefix = "ACP_GO_OPENCODEV2_INTERNAL_"
const CarrierKey = "acp-go-opencodev2"

//go:embed environment.mjs
var environmentPlugin string

func ModelSelectionShapeError(value string) error {
	provider, id, ok := strings.Cut(value, "/")
	if !ok || provider == "" || id == "" || strings.ContainsAny(value, " \t\r\n\x00") {
		return errors.New("model must be provider-qualified")
	}

	return nil
}

func HomeEnvironment(home string) map[string]string {
	owned := map[string]string{}

	if home != "" {
		for key, dir := range map[string]string{"XDG_DATA_HOME": "data", "XDG_CONFIG_HOME": "config", "XDG_CACHE_HOME": "cache", "XDG_STATE_HOME": "state"} {
			owned[key] = filepath.Join(home, dir)
		}
	}

	return owned
}

func DataDir(lookup func(string) (string, bool)) string {
	if value, _ := lookup("XDG_DATA_HOME"); value != "" {
		return filepath.Join(value, "opencode")
	}

	home, _ := lookup("HOME")

	return filepath.Join(home, ".local", "share", "opencode")
}

func ConfigDir(lookup func(string) (string, bool)) string {
	if value, _ := lookup("XDG_CONFIG_HOME"); value != "" {
		return filepath.Join(value, "opencode")
	}

	home, _ := lookup("HOME")

	return filepath.Join(home, ".config", "opencode")
}

// WritePlugin registers the environment hook alongside the caller's native config.
func WritePlugin(root, config string, ownsHome bool) (string, error) {
	proof, _ := json.Marshal(filepath.Join(root, "ready"))

	keys := []string{"OPENCODE_SERVER_USERNAME", "OPENCODE_SERVER_PASSWORD", "OPENCODE_CONFIG_CONTENT"}
	if ownsHome {
		keys = append(keys, "XDG_DATA_HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME")
	}

	encoded, _ := json.Marshal(keys)
	source := strings.ReplaceAll(environmentPlugin, "__PROOF_ROOT__", string(proof))
	source = strings.ReplaceAll(source, "__OWNED_KEYS__", string(encoded))

	path := filepath.Join(root, "index.js")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		return "", err
	}

	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"name":"acp-go-opencodev2-environment","type":"module"}`), 0o600); err != nil {
		return "", err
	}

	values := map[string]any{}
	if config != "" {
		if err := json.Unmarshal([]byte(config), &values); err != nil || values == nil {
			return "", errors.New("invalid OPENCODE_CONFIG_CONTENT")
		}
	}

	plugins, ok := values["plugins"].([]any)
	if values["plugins"] != nil && !ok {
		return "", errors.New("invalid native plugin list")
	}

	values["plugins"] = append(plugins, (&url.URL{Scheme: "file", Path: filepath.ToSlash(root)}).String())
	data, err := json.Marshal(values)

	return string(data), err
}

func CheckPlugin(root, directory string) error {
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return err
	}

	hash := sha256.Sum256([]byte(canonical))

	data, err := os.ReadFile(filepath.Join(root, "ready", hex.EncodeToString(hash[:])))
	if err != nil || string(data) != canonical {
		return errors.New("native session environment plugin did not load")
	}

	return nil
}
