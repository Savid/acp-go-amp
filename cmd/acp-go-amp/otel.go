package main

import (
	"context"
	"log/slog"

	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-core/observer/exporters"
)

// telemetryConfig is the bundle of agent options and the logger the exporter
// configuration produced.
type telemetryConfig struct {
	logger   *slog.Logger
	options  []ampacp.Option
	shutdown func(context.Context) error
}

// configureTelemetry maps the providers core configured from OTEL_* onto the
// agent's own options.
func configureTelemetry(ctx context.Context, baseLogger *slog.Logger, version string) (telemetryConfig, error) {
	bundle, err := exporters.Configure(ctx, exporters.Config{Vendor: "amp", Version: version, Logger: baseLogger})
	if err != nil {
		return telemetryConfig{}, err
	}

	config := telemetryConfig{logger: bundle.Logger, shutdown: bundle.Shutdown}
	if bundle.Propagator != nil {
		config.options = append(config.options, ampacp.WithTextMapPropagator(bundle.Propagator))
	}

	if bundle.TracerProvider != nil {
		config.options = append(config.options, ampacp.WithTracerProvider(bundle.TracerProvider))
	}

	if bundle.MeterProvider != nil {
		config.options = append(config.options, ampacp.WithMeterProvider(bundle.MeterProvider))
	}

	return config, nil
}
