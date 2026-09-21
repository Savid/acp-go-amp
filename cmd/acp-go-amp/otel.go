package main

import (
	"context"
	"log/slog"

	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-core/observer/exporters"
)

// configureTelemetry builds the exporters the OTEL_* environment enables and
// maps the configured providers onto the agent's options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (exporters.Bundle, []ampacp.Option, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "amp", Version: version, Logger: baseLogger})
	if err != nil {
		return exporters.Bundle{}, nil, err
	}

	options := []ampacp.Option{ampacp.WithTextMapPropagator(bundle.Propagator)}
	if bundle.TracerProvider != nil {
		options = append(options, ampacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		options = append(options, ampacp.WithMeterProvider(bundle.MeterProvider))
	}

	return bundle, options, nil
}
