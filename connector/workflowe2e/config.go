package workflowe2e

import (
	"fmt"
	"time"
)

// Config definisce la configurazione del connector workflowe2e.
// Viene caricata dal Collector tramite mapstructure.
type Config struct {
	/* Può essere utile per traces con più workflow
	// WorkflowAttribute è l'attributo (span o resource) usato
	// per identificare il workflow a cui appartiene una trace.
	// NON deve essere trace_id.
	WorkflowAttribute string `mapstructure:"workflow_attribute"`
	*/

	// E2ELatencyMetricName è il nome della metrica Prometheus
	// che espone la latenza E2E del workflow (in millisecondi)
	E2ELatencyMetricName string `mapstructure:"e2e_latency_metric_name"`

	// ServiceLatencyMetricName è il nome della metrica Prometheus
	// che espone la latenza per singolo servizio (in millisecondi).
	ServiceLatencyMetricName string `mapstructure:"service_latency_metric_name"`

	// ServiceLatencyMode va messa a true se è richiesto di calcolare ed esporre anche le latenze
	// per i singoli microservizi
	ServiceLatencyMode bool `mapstructure:"service_latency_mode"`

	// ServiceNameAttribute è l'attributo (span o resource)
	// che identifica il nome del microservizio (per distinguerli).
	// Es.: "service.name"
	ServiceNameAttribute string `mapstructure:"service_name_attribute"`

	// Se true, significa che sta usando sia OTel che Istio per generare spans, altrimenti solo OTel
	UsingIstio bool `mapstructure:"using_istio"`

	// TraceIdleTimeout è il tempo di inattività dopo il quale una trace
	// viene considerata completa e finalizzata (es. "30s").
	TraceIdleTimeout time.Duration `mapstructure:"trace_idle_timeout"`

	// TraceFlushInterval è la frequenza con cui il connector controlla
	// le traces e finalizza quelle scadute (es. "5s").
	TraceFlushInterval time.Duration `mapstructure:"trace_flush_interval"`
}

// Validate verifica la correttezza della configurazione.
// Viene chiamata automaticamente dal Collector all'avvio.
func (c *Config) Validate() error {
	/*
		if c.WorkflowAttribute == "" {
			return fmt.Errorf("workflow_attribute must not be empty")
		}
	*/
	if c.E2ELatencyMetricName == "" {
		return fmt.Errorf("e2e_latency_metric_name must not be empty")
	}

	if c.ServiceLatencyMode && c.ServiceLatencyMetricName == "" {
		return fmt.Errorf("service_latency_metric_name must not be empty when service_latency_mode != none")
	}

	if c.ServiceLatencyMode && c.ServiceNameAttribute == "" {
		return fmt.Errorf("service_name_attribute must not be empty")
	}

	// Convalida dei timeout
	if c.TraceIdleTimeout < 0 {
		return fmt.Errorf("trace_idle_timeout must be >= 0")
	}
	if c.TraceFlushInterval < 0 {
		return fmt.Errorf("trace_flush_interval must be >= 0")
	}

	return nil
}
