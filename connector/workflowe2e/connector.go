package workflowe2e

import (
	"context"
	"errors"
	"sort"
	"sync"
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

// Stato cumulativo di un histogram
type histogramState struct {
	mu      sync.Mutex // mutex per thread-safety
	count   uint64     // totale osservazioni
	sumNs   uint64     // totale latenza
	buckets []uint64   // buckets cumulativi
}

// connectorImp implements connector.Traces which means it must implement:
// - component.Component (Start, Shutdown)
// - consumer.Traces (ConsumeTraces)
// Struttura del connector con i parametri desiderati
type connectorImp struct {
	metricsConsumer consumer.Metrics
	cfg             *Config
	logger          *zap.Logger

	// Stato cumulativo degli histogram
	histMu    sync.RWMutex
	histState map[string]*histogramState // Ogni combinazione di label è una time series diversa

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
		return nil, errors.New("Invalid config, expected *workflowe2e.Config")
	}

	return &connectorImp{
		metricsConsumer: metricsConsumer,
		cfg:             myCfg,
		logger:          settings.TelemetrySettings.Logger,
		histState:       make(map[string]*histogramState),
	}, nil
}

// Definisce le capacità del connector, in particolare se modifica i dati ricevuti o no
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Assunzione: upstream è presente groupbytrace, quindi td contiene gli spans della stessa trace.
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {

	c.logger.Debug("DEBUG_LOGS: ConsumeTraces called",
		zap.Int("resource_spans_len", td.ResourceSpans().Len()),
	)

	c.logger.Debug("DEBUG_LOGS: ConsumeTraces called",
		zap.Int("span_count", td.SpanCount()),
	)

	traceIDs := make(map[string]struct{})

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				tid := ss.Spans().At(k).TraceID().String()
				traceIDs[tid] = struct{}{}
			}
		}
	}

	c.logger.Debug("DEBUG_LOGS: trace ids summary",
		zap.Int("unique_trace_count", len(traceIDs)),
		zap.Any("trace_ids", keys(traceIDs)),
	)

	latencyMs, serviceActiveNs, err := c.calculateE2ELatency(td, c.cfg)
	/*if err != nil {
		return nil
	}*/

	if err == nil {
		// prova a estrarre un trace_id rappresentativo (se presente)
		traceID := ""
		if td.ResourceSpans().Len() > 0 {
			rs := td.ResourceSpans().At(0)
			if rs.ScopeSpans().Len() > 0 {
				ss := rs.ScopeSpans().At(0)
				if ss.Spans().Len() > 0 {
					traceID = ss.Spans().At(0).TraceID().String()
				}
			}
		}
		c.logger.Debug("DEBUG_LOGS: E2E latency computed",
			zap.String("trace_id", traceID),
			zap.Float64("latency_ms", latencyMs),
			zap.Int("num_services", len(serviceActiveNs)),
		)
	} else {
		c.logger.Debug("PROVA calculateE2ELatency error", zap.Error(err))
		return nil
	}

	attrs := td.ResourceSpans().At(0).Resource().Attributes()
	attrs.Range(func(k string, v pcommon.Value) bool {
		c.logger.Debug("DEBUG_LOGS: resource attributes",
			zap.String("key", k),
			zap.String("value", v.AsString()),
		)
		return true
	})

	scope := td.ResourceSpans().At(0).ScopeSpans().At(0).Scope().Attributes()
	scope.Range(func(k string, v pcommon.Value) bool {
		c.logger.Debug("DEBUG_LOGS: scope attributes",
			zap.String("key", k),
			zap.String("value", v.AsString()),
		)
		return true
	})

	spanAttrs := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
	spanAttrs.Range(func(k string, v pcommon.Value) bool {
		c.logger.Debug("DEBUG_LOGS: span attributes",
			zap.String("trace_id", td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID().String()),
			zap.String("span_name", td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()),
			zap.String("key", k),
			zap.String("value", v.AsString()),
		)
		return true
	})

	md := pmetric.NewMetrics()               // Creazione del contenitore Metrics (root)
	rm := md.ResourceMetrics().AppendEmpty() // Aggiunge un ResourceMetrics. Qui potresti aggiungere anche attributi di resource, es. rm.Resource().Attributes().PutStr("service.name", "workflow-e2e")
	sm := rm.ScopeMetrics().AppendEmpty()    // Rappresenta la libreria di strumentazione. Di solito nei connector si lascia vuota

	metric := sm.Metrics().AppendEmpty() // Qui definisci la metrica, con nome metrica → esposta a Prometheus e (opzionale) unit, description
	metric.SetName(c.cfg.E2ELatencyMetricName)
	metric.SetDescription("End-to-end workflow latency")
	metric.SetUnit("s")

	// Buckets bounds (s)
	bounds := []float64{0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.4, 2.0, 5.0, 10.0, 15.0}

	// Key fissa per la latenza E2E
	const e2eKey = "__e2e__"

	// Espongo la latenza e2e come metrica, di tipo gauge o histogram
	if c.cfg.EnableHistogram {

		// Osserva la latenza (aggiorna stato cumulativo in memoria)
		c.updateHistogram(e2eKey, uint64(latencyMs*1e6), bounds)

		// Snapshot consistente dello stato cumulativo
		hs := c.getHistogramState(e2eKey, len(bounds)+1)
		hs.mu.Lock()
		count := hs.count
		sumSec := float64(hs.sumNs) / 1e9
		buckets := append([]uint64(nil), hs.buckets...)
		hs.mu.Unlock()

		h := metric.SetEmptyHistogram()
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

		dp.ExplicitBounds().FromRaw(bounds)
		dp.SetCount(count)
		dp.SetSum(sumSec)
		dp.BucketCounts().FromRaw(buckets)

		dp.Attributes().PutStr("service_name", e2eKey)

	} else {
		g := metric.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(latencyMs / 1000.0)
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
		dp.Attributes().PutStr("service_name", e2eKey)
	}

	// Espongo la latenza per i singoli microservizi come metrica, di tipo gauge o histogram
	if c.cfg.ServiceLatencyMode != "none" {

		for svc, activeNs := range serviceActiveNs {

			svcLatencyMs := float64(activeNs) / 1e6

			m := sm.Metrics().AppendEmpty()
			m.SetName(c.cfg.ServiceLatencyMetricName)
			m.SetDescription("Per-service latency (active time)")
			m.SetUnit("s")

			if c.cfg.EnableHistogram {

				key := "service:" + svc

				// Osserva la latenza del servizio
				c.updateHistogram(key, activeNs, bounds)

				// Snapshot consistente dello stato cumulativo
				hs := c.getHistogramState(key, len(bounds)+1)
				hs.mu.Lock()
				count := hs.count
				sumSec := float64(hs.sumNs) / 1e9
				buckets := append([]uint64(nil), hs.buckets...)
				hs.mu.Unlock()

				h := m.SetEmptyHistogram()
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

				dp.ExplicitBounds().FromRaw(bounds)
				dp.SetCount(count)
				dp.SetSum(sumSec)
				dp.BucketCounts().FromRaw(buckets)

				// Aggiunge una label alla metrica, con il nome del servizio
				dp.Attributes().PutStr("service_name", svc)

			} else {
				g := m.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(svcLatencyMs / 1000.0)
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
				dp.Attributes().PutStr("service_name", svc)
			}
		}
	}

	// Invia le metriche (la struttura gerarchica creata (albero)) alla pipeline metrics
	if err := c.metricsConsumer.ConsumeMetrics(ctx, md); err != nil {
		if c.logger != nil {
			c.logger.Error("Failed to consume metrics", zap.Error(err))
		}
		return err
	}

	return nil
}

// intervallo usato per il merge degli spans di uno stesso servizio
// Contiene il tempo di inizio e fine per uno span
type interval struct {
	start pcommon.Timestamp
	end   pcommon.Timestamp
}

// Calcola la latenza end-to-end (ms) di una trace
// Restituisce inoltre una mappa service -> durata attiva (ns)
// NB: td è assunto come "trace completa" (groupbytrace upstream).
func (c *connectorImp) calculateE2ELatency(td ptrace.Traces, cfg *Config) (float64, map[string]uint64, error) {

	if td.ResourceSpans().Len() == 0 {
		return 0, nil, errors.New("No spans available")
	}

	var minStart pcommon.Timestamp = maxTimestamp
	var maxEnd pcommon.Timestamp = 0

	serviceIntervals := make(map[string][]interval)
	enableService := cfg.ServiceLatencyMode != "none"

	allowListMode := cfg.ServiceLatencyMode == "list"
	allowMap := make(map[string]struct{}, len(cfg.ServiceAllowList))
	if allowListMode {
		for _, s := range cfg.ServiceAllowList {
			allowMap[s] = struct{}{}
		}
	}

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				start := span.StartTimestamp()
				end := span.EndTimestamp()

				c.logger.Debug("DEBUG_LOGS: span seen",
					zap.String("trace_id", span.TraceID().String()),
					zap.String("span_name", span.Name()),
					zap.Int64("start_ns", int64(start)),
					zap.Int64("end_ns", int64(end)),
				)

				if end == 0 {
					continue
				}

				if start < minStart {
					minStart = start
				}
				if end > maxEnd {
					maxEnd = end
				}

				if !enableService {
					continue
				}

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

				if allowListMode {
					if _, ok := allowMap[svc]; !ok {
						continue
					}
				}

				// Alla fine del loop, per ogni servizio avrò tutti gli intervalli temporali dei suoi spans
				serviceIntervals[svc] = append(serviceIntervals[svc], interval{start, end})
			}
		}
	}

	serviceActiveNs := make(map[string]uint64)
	// Per ogni servizio, fa il merge degli intervalli per calcolare la latenza
	for svc, ivs := range serviceIntervals {

		// Ordina gli intervalli di uno span sulla base dell'istante di inizio
		sort.Slice(ivs, func(i, j int) bool {
			return ivs[i].start < ivs[j].start
		})

		cur := ivs[0] // Consideriamo il primo intervallo
		var total uint64

		// Loop per il merge, prende ogni intervallo successivo e lo confronti con cur
		for i := 1; i < len(ivs); i++ {
			// Controlla se l'intervallo inizia mentre uno è ancora attivo (lo span non è terminato)
			if ivs[i].start <= cur.end {
				if ivs[i].end > cur.end { // Se lo è, controlla se finisce dopo
					cur.end = ivs[i].end // In tal caso aggiorno il tempo di fine
				}
			} else { // Se no, salvo il tempo trascorso in attività e passo al nuovo intervallo
				total += uint64(cur.end - cur.start)
				cur = ivs[i]
			}
		}
		total += uint64(cur.end - cur.start)

		serviceActiveNs[svc] = total
	}

	if minStart == maxTimestamp || maxEnd == 0 {
		return 0, nil, errors.New("Invalid timestamps")
	}

	latencyMs := float64(maxEnd-minStart) / 1e6

	c.logger.Debug("DEBUG_LOGS: trace times summary",
		zap.String("minStart_time", time.Unix(0, int64(minStart)).Format(time.RFC3339Nano)),
		zap.String("maxEnd_time", time.Unix(0, int64(maxEnd)).Format(time.RFC3339Nano)),
		zap.Float64("latency_ms_calc", latencyMs),
	)
	return latencyMs, serviceActiveNs, nil
}

// Funzione per prendere lo stato (count, sum, buckets) attuale dell'istogramma
// Restituisce lo stato cumulativo di un histogram identificato da key, se lo stato non esiste ancora, lo crea
func (c *connectorImp) getHistogramState(key string, bucketCount int) *histogramState {
	// Acquisisce un lock in lettura (RLock) sul histState map. Recupera lo stato dell’histogram per la key e rilascia il lock
	c.histMu.RLock()
	hs := c.histState[key]
	c.histMu.RUnlock()
	if hs != nil { // Se lo stato esiste già lo restituisce, altrimenti lo crea
		return hs
	}

	// Se lo stato non esiste, acquisisce un lock in scrittura per creare un nuovo stato in sicurezza
	// defer assicura che il lock venga rilasciato alla fine della funzione
	c.histMu.Lock()
	defer c.histMu.Unlock()

	// Double-check locking: nel tempo tra RUnlock e Lock, un’altra goroutine potrebbe aver creato lo stato.
	// Se ora lo stato esiste, lo restituisce senza ricrearlo
	if hs = c.histState[key]; hs != nil {
		return hs
	}

	// Crea un nuovo histogramState vuoto: count e sumNs sono implicitamente 0.
	// buckets è una slice di lunghezza bucketCount, inizializzata a zero
	hs = &histogramState{
		buckets: make([]uint64, bucketCount),
	}
	c.histState[key] = hs // Memorizza il nuovo stato nella mappa

	return hs
}

// Aggiorna lo stato cumulativo di un istogramma (identificato da una chiave)
func (c *connectorImp) updateHistogram(key string, latencyNs uint64, bounds []float64) {
	hs := c.getHistogramState(key, len(bounds)+1) // Recupera lo stato corrente dell’histogram per la chiave key. Se non esiste, lo crea. il +1 serve per il bound +inf

	// Aggiorno lo stato dell'istogramma
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.count++
	hs.sumNs += latencyNs

	latencySec := float64(latencyNs) / 1e9
	idx := len(bounds) // +Inf bucket
	// Controlla in quale bucket inserire la latenza
	for i, b := range bounds { // NB: OTLP/Prometheus cumulano automaticamente i buckets poi
		if latencySec <= b {
			idx = i
			break
		}
	}
	hs.buckets[idx]++

	c.logger.Debug("DEBUG_LOGS: histogram update",
		zap.String("key", key),
		zap.Uint64("count", hs.count),
		zap.Uint64("sum_ns", hs.sumNs),
		zap.Any("buckets", hs.buckets),
	)
}

// Opzionale, ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	if c.logger != nil {
		c.logger.Info("DEBUG_LOGS: connector starting",
			zap.String("metric_name_e2e", c.cfg.E2ELatencyMetricName), // se hai una Version nel cfg
		)
	}
	return nil
}

// Opzionale, ridefinizione del metodo per chiudere il connector
func (c *connectorImp) Shutdown(ctx context.Context) error {
	return nil
}
