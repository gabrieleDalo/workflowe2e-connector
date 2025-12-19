package workflowe2e

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var maxTimestamp = pcommon.Timestamp(^uint64(0))

// connectorImp implements connector.Traces which means it must implement:
// - component.Component (Start, Shutdown)
// - consumer.Traces (ConsumeTraces)
// Struttura del connector con i parametri desiderati
type connectorImp struct {
	metricsConsumer consumer.Metrics
	cfg             *Config
	logger          *zap.Logger

	// Includi anche i seguenti parametri se non vuoi un'implementazione specifica per i metodi Start e Shutdown
	//component.Start
	//component.Shutdown
}

// createTracesToMetricsConnector della factory ha la firma corretta:
// func(context.Context, connector.Settings, component.Config, consumer.Metrics) (connector.Traces, error)
// Funzione per creare un nuovo connector
func newConnector(
	_ context.Context,
	settings connector.Settings,
	cfg component.Config,
	metricsConsumer consumer.Metrics,
) (connector.Traces, error) {

	// type-assert la config al tuo tipo specifico
	myCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("invalid config, expected *workflowe2e.Config")
	}

	return &connectorImp{
		metricsConsumer: metricsConsumer,
		cfg:             myCfg,
		logger:          settings.TelemetrySettings.Logger,
	}, nil
}

// Definisce le capacità del connector, in particolare se modifica i dati ricevuti o no
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Assunzione: upstream è presente groupbytrace, quindi td contiene gli spans della stessa trace.
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {

	latencyMs, serviceRanges, err := calculateE2ELatency(td, c.cfg)

	if err != nil {
		return nil
	}

	md := pmetric.NewMetrics()               // Creazione del contenitore Metrics (root)
	rm := md.ResourceMetrics().AppendEmpty() // Aggiunge un ResourceMetrics. Qui potresti aggiungere anche attributi di resource, es. rm.Resource().Attributes().PutStr("service.name", "workflow-e2e")
	sm := rm.ScopeMetrics().AppendEmpty()    // Rappresenta la libreria di strumentazione. Di solito nei connector si lascia vuota

	metric := sm.Metrics().AppendEmpty() // Qui definisci la metrica, con nome metrica → esposta a Prometheus e (opzionale) unit, description
	metric.SetName(c.cfg.E2ELatencyMetricName)
	metric.SetDescription("End-to-end workflow latency")
	metric.SetUnit("ms")

	// Espongo la latenza e2e come metrica, di tipo gauge o histogram
	if c.cfg.EnableHistogram {
		h := metric.SetEmptyHistogram()                           // Specifica il tipo di metrica, in questo caso un Histogram. OpenTelemetry metric può essere SOLO uno tra: Gauge, Sum (counter), Histogram
		dp := h.DataPoints().AppendEmpty()                        // Un histogram può avere più datapoint. Ogni datapoint rappresenta: una combinazione di label ed una finestra temporale
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano())) // Prometheus non richiede timestamp, ma è corretto metterlo, imposta quando è avvenuta l'osservazione

		// Buckets decisi
		bounds := []float64{10, 50, 100, 500, 1000, 5000} // Definisco i limiti superiori dei buckets. Es. [-infinito, 10], [10, 50], ... , [5000, +infinito]
		dp.ExplicitBounds().FromRaw(bounds)
		dp.SetCount(1)       // Indica che è stato osservato 1 evento in questo datapoint, se avessi 5 trace aggregate insieme → SetCount(5)
		dp.SetSum(latencyMs) // Indica la somma dei valori osservati

		rawBounds := dp.ExplicitBounds().AsRaw()
		counts := make([]uint64, len(rawBounds)+1) // Crea un array di contatori, ogni posizione rappresenta quanti eventi sono finiti in quel bucket
		placed := false
		// Ciclo per determinare il bucket corretto in cui inserire la latenza
		for i, b := range rawBounds {
			if latencyMs <= b {
				counts[i] = 1
				placed = true
				break
			}
		}
		// Caso +Inf, se il valore è maggiore dell’ultimo bound allora la latenza finisce nel bucket +Inf
		if !placed {
			counts[len(counts)-1] = 1
		}
		dp.BucketCounts().FromRaw(counts)

	} else {
		g := metric.SetEmptyGauge()                               // Specifica il tipo di metrica, in questo caso un gauge
		dp := g.DataPoints().AppendEmpty()                        // Una Gauge può avere più datapoint (per label diverse)
		dp.SetDoubleValue(latencyMs)                              // Valore numerico (float64) per la latenza
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano())) // Prometheus non richiede timestamp, ma è corretto metterlo, imposta quando è avvenuta l'osservazione
	}

	// Espongo la latenza per i singoli microservizi come metrica, di tipo gauge o histogram
	if c.cfg.ServiceLatencyMode != "none" && len(serviceRanges) > 0 {

		bounds := []float64{10, 50, 100, 500, 1000, 5000}

		for svc, rng := range serviceRanges {
			// sanity check range
			if rng[0] == maxTimestamp || rng[1] == 0 {
				continue
			}
			svcLatencyMs := float64(rng[1]-rng[0]) / 1e6

			m := sm.Metrics().AppendEmpty()
			m.SetName(c.cfg.ServiceLatencyMetricName)
			m.SetDescription("Per-service latency")
			m.SetUnit("ms")

			if c.cfg.EnableHistogram {
				h := m.SetEmptyHistogram()
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
				dp.ExplicitBounds().FromRaw(bounds)
				dp.SetCount(1)
				dp.SetSum(svcLatencyMs)

				counts := make([]uint64, len(bounds)+1)
				rawBounds := dp.ExplicitBounds().AsRaw()
				placed := false
				for i, b := range rawBounds {
					if svcLatencyMs <= b {
						counts[i] = 1
						placed = true
						break
					}
				}
				if !placed {
					counts[len(counts)-1] = 1
				}
				dp.BucketCounts().FromRaw(counts)

				// label service
				dp.Attributes().PutStr("service", svc)
			} else {
				g := m.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(svcLatencyMs)
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
				dp.Attributes().PutStr("service", svc)
			}
		}
	}

	// Invia le metriche (la struttura gerarchica creata (albero)) alla pipeline metrics (che le esporterà a Prometheus)
	// metricsConsumer è il prossimo componente nella pipeline, nel mio caso → exporter prometheus
	// il Collector si occupa di: buffering, retry, esposizione su /metrics
	// Qui non si parla mai direttamente con Prometheus
	if err := c.metricsConsumer.ConsumeMetrics(ctx, md); err != nil {
		// log e ritenta upstream; qui ritorniamo l'errore
		if c.logger != nil {
			c.logger.Error("failed to consume metrics", zap.Error(err))
		}
		return err
	}

	return nil
}

// Calcola la latenza end-to-end (ms) di una trace
// Restituisce inoltre una mappa service -> [minStart, maxEnd] per i servizi per i quali ci interessa esporre la latenza
// NB: td è assunto come "trace completa" (groupbytrace upstream).
func calculateE2ELatency(td ptrace.Traces, cfg *Config) (float64, map[string][2]pcommon.Timestamp, error) {
	// Nulla da fare se non ci sono spans
	if td.ResourceSpans().Len() == 0 {
		return 0, nil, errors.New("no spans")
	}

	// Inizializza minStart e maxEnd
	var minStart pcommon.Timestamp = maxTimestamp
	var maxEnd pcommon.Timestamp = 0

	// Prepara la mappa dei servizi (solo se richiesto)
	serviceRanges := make(map[string][2]pcommon.Timestamp)
	enableService := cfg.ServiceLatencyMode != "none"

	// Helper per check allowlist (se mode == "list")
	allowListMode := cfg.ServiceLatencyMode == "list"
	// converti allowlist in map per lookup O(1) se è grande (micro-ottimizzazione)
	allowMap := make(map[string]struct{}, len(cfg.ServiceAllowList))
	if allowListMode {
		for _, s := range cfg.ServiceAllowList {
			allowMap[s] = struct{}{}
		}
	}

	// itera su ResourceSpans -> ScopeSpans -> Spans
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				start := span.StartTimestamp()
				end := span.EndTimestamp()

				// Skip span senza end (incompleto)
				if end == 0 {
					continue
				}

				if start < minStart {
					minStart = start
				}
				if end > maxEnd {
					maxEnd = end
				}

				// Se non dobbiamo calcolare per-service, salta la raccolta del servizio
				if !enableService {
					continue
				}

				// Estraiamo il nome del servizio (prima Istio span attribute se richiesto, poi resource attr)
				var svc string
				if cfg.UsingIstio {
					if v, ok := span.Attributes().Get(cfg.ServiceNameAttribute); ok {
						svc = v.AsString()
					}
				}
				if svc == "" {
					if v, ok := rs.Resource().Attributes().Get(cfg.ServiceNameAttribute); ok {
						svc = v.AsString()
					}
				}
				if svc == "" {
					svc = "unknown"
				}

				// Se siamo in allow-list mode, verifica
				if allowListMode {
					if _, ok := allowMap[svc]; !ok {
						// non ci interessa questo servizio
						continue
					}
				}

				// Ora aggiorna range per servizio (minStart, maxEnd)
				if rng, ok := serviceRanges[svc]; ok {
					if start < rng[0] {
						rng[0] = start
					}
					if end > rng[1] {
						rng[1] = end
					}
					serviceRanges[svc] = rng
				} else {
					serviceRanges[svc] = [2]pcommon.Timestamp{start, end}
				}
			}
		}
	}

	if minStart == maxTimestamp || maxEnd == 0 {
		return 0, nil, errors.New("invalid timestamps")
	}

	latencyMs := float64(maxEnd-minStart) / 1e6
	return latencyMs, serviceRanges, nil
}

// Opzionale, ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	// se vuoi tenere host o creare background worker, fallo qui
	// example: c.host = host
	return nil
}

// Opzionale, ridefinizione del metodo per chiudere il connector
func (c *connectorImp) Shutdown(ctx context.Context) error {
	// pulisci risorse, cancella goroutine, flush buffer, ecc.
	return nil
}
