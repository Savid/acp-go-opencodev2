package opencodeacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionDefaults(t *testing.T) {
	t.Parallel()

	options := applyOptions(nil)
	require.Equal(t, "acp-go-opencodev2", options.AgentName)
	require.Equal(t, int64(6291456), options.ImageLimits.MaxInputBytesPerImage)
	require.Equal(t, defaultMaxActiveSessions, options.ConcurrencyLimits.MaxActiveSessions)
	require.Equal(t, defaultMaxConcurrentClientCalls, options.ConcurrencyLimits.MaxConcurrentClientCalls)

	options = applyOptions([]Option{
		WithAgentName("n"), WithAgentTitle("t"), WithAgentVersion("v"), WithExecutablePath("/p"), WithHome("/h"),
		WithScratchDir("/s"), WithInputHandoffRoot("/r"), WithDefaultModel("a/b"), WithConfiguredModels([]string{"a/b"}),
		WithEnv(map[string]string{"A": "1"}),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2, MaxConcurrentClientCalls: 3}),
		WithImageLimits(ImageLimits{}), WithSeedFiles(map[string]string{"a": "b"}),
		WithTracerProvider(nil), WithMeterProvider(nil), WithTextMapPropagator(nil), WithLogger(nil), WithSessionStore(nil),
	})
	require.Equal(t, "n", options.AgentName)
	require.Equal(t, "t", options.AgentTitle)
	require.Equal(t, "v", options.AgentVersion)
	require.Equal(t, "/p", options.ExecutablePath)
	require.Equal(t, "/h", options.Home)
	require.Equal(t, "/s", options.ScratchDir)
	require.Equal(t, "/r", options.InputHandoffRoot)
	require.Equal(t, "a/b", options.DefaultModel)
	require.Equal(t, []string{"a/b"}, options.ConfiguredModels)
	require.Equal(t, map[string]string{"A": "1"}, options.Env)
	require.Equal(t, 2, options.ConcurrencyLimits.MaxActiveSessions)
	require.Equal(t, int64(0), options.ImageLimits.MaxInputBytesPerImage)
	require.Equal(t, map[string]string{"a": "b"}, options.SeedFiles)
}

func TestValidateConfiguredModels(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateConfiguredModels([]string{"a/b", "c/d"}))
	require.Error(t, validateConfiguredModels([]string{"a/b", "a/b"}))
	require.Error(t, validateConfiguredModels([]string{" a/b"}))
	require.Error(t, validateConfiguredModels([]string{""}))
	require.Error(t, validateConfiguredModels([]string{"nope"}))
}
