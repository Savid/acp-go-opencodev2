package main

import (
	"context"
	"log/slog"

	"github.com/savid/acp-go-core/observer/exporters"
	opencodeacp "github.com/savid/acp-go-opencodev2"
)

// configureTelemetry builds the exporters the OTEL_* environment enables and
// maps the configured providers onto the agent's options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (exporters.Bundle, []opencodeacp.Option, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "opencode", Version: version, Logger: baseLogger})
	if err != nil {
		return exporters.Bundle{}, nil, err
	}

	options := []opencodeacp.Option{opencodeacp.WithTextMapPropagator(bundle.Propagator)}
	if bundle.TracerProvider != nil {
		options = append(options, opencodeacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, opencodeacp.WithMeterProvider(bundle.MeterProvider))
	}

	return bundle, options, nil
}
