package workflowe2e

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const typeStr = "workflowe2e" // Indica il nome (tipo) del connector

// Crea una nuova factory per il connector
func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		connector.WithTracesToMetrics(
			createTracesToMetricsConnector,
			component.StabilityLevelAlpha,
		),
	)
}

// Restituisce la configurazione di default (valori di default per gli attributi) del connector
func createDefaultConfig() component.Config {
	return &Config{
		E2ELatencyMetricName:     "workflow_e2e_latency",
		ServiceLatencyMetricName: "workflow_service_latency",
		ServiceLatencyMode:       false,          // default solo latenza E2E
		ServiceNameAttribute:     "service.name", // default per OTel spans
		ExperimentNameAttribute:  "experiment.name",
		UsingIstio:               false,            // default: solo per OTel
		TraceIdleTimeout:         15 * time.Second, // 30s
		TraceFlushInterval:       3 * time.Second,  // 5s
	}
}

// Crea il connector, che consuma traces e produce metriche
func createTracesToMetricsConnector(ctx context.Context, params connector.Settings, cfg component.Config, nextConsumer consumer.Metrics) (connector.Traces, error) {
	return newConnector(ctx, params, cfg, nextConsumer)
}
