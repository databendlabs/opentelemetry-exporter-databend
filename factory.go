// Copyright The Databend Exporter Authors
// SPDX-License-Identifier: Apache-2.0

// Package databendexporter exports OpenTelemetry logs, traces and metrics to Databend.
package databendexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var componentType = component.MustNewType("databend")

// NewFactory creates the Databend exporter factory. All signals are in development.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(componentType, func() component.Config { return createDefaultConfig() },
		exporter.WithLogs(createLogs, component.StabilityLevelDevelopment),
		exporter.WithTraces(createTraces, component.StabilityLevelDevelopment),
		exporter.WithMetrics(createMetrics, component.StabilityLevelDevelopment))
}

func options(e *databendExporter) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		// The HTTP client's timeout applies to each attempt, including the response body.
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: e.cfg.ClientConfig.Timeout}),
		exporterhelper.WithQueue(e.cfg.QueueSettings),
		exporterhelper.WithRetry(e.cfg.BackOffConfig),
	}
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	e := &databendExporter{cfg: cfg.(*Config), settings: set.TelemetrySettings, tableName: cfg.(*Config).LogsTable}
	return exporterhelper.NewLogs(ctx, set, cfg, e.pushLogs, options(e)...)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	e := &databendExporter{cfg: cfg.(*Config), settings: set.TelemetrySettings, tableName: cfg.(*Config).TracesTable}
	return exporterhelper.NewTraces(ctx, set, cfg, e.pushTraces, options(e)...)
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	e := &databendExporter{cfg: cfg.(*Config), settings: set.TelemetrySettings, tableName: cfg.(*Config).MetricsTable}
	return exporterhelper.NewMetrics(ctx, set, cfg, e.pushMetrics, options(e)...)
}
