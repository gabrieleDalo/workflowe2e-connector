/*
package workflowe2e

import (
	"context"
	"errors"
	"sort"
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

	latencyMs, serviceActiveNs, err := c.calculateE2ELatency(td, c.cfg)
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
		h := metric.SetEmptyHistogram()
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

		bounds := []float64{10, 50, 100, 500, 1000, 5000}
		dp.ExplicitBounds().FromRaw(bounds)
		dp.SetCount(1)
		dp.SetSum(latencyMs)

		counts := make([]uint64, len(bounds)+1)
		for i, b := range bounds {
			if latencyMs <= b {
				counts[i] = 1
				break
			}
		}
		dp.BucketCounts().FromRaw(counts)

	} else {
		g := metric.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(latencyMs)
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
	}

	// Espongo la latenza per i singoli microservizi come metrica, di tipo gauge o histogram
	if c.cfg.ServiceLatencyMode != "none" {

		bounds := []float64{10, 50, 100, 500, 1000, 5000}

		for svc, activeNs := range serviceActiveNs {

			svcLatencyMs := float64(activeNs) / 1e6

			m := sm.Metrics().AppendEmpty()
			m.SetName(c.cfg.ServiceLatencyMetricName)
			m.SetDescription("Per-service latency (active time)")
			m.SetUnit("ms")

			if c.cfg.EnableHistogram {
				h := m.SetEmptyHistogram()
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
				dp.ExplicitBounds().FromRaw(bounds)
				dp.SetCount(1)
				dp.SetSum(svcLatencyMs)

				counts := make([]uint64, len(bounds)+1)
				for i, b := range bounds {
					if svcLatencyMs <= b {
						counts[i] = 1
						break
					}
				}
				dp.BucketCounts().FromRaw(counts)

				// Aggiunge una label alla metrica, con il nome del servizio
				dp.Attributes().PutStr("service-name", svc)
			} else {
				g := m.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(svcLatencyMs)
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))
				dp.Attributes().PutStr("service-name", svc)
			}
		}
	}

	// Invia le metriche (la struttura gerarchica creata (albero)) alla pipeline metrics
	if err := c.metricsConsumer.ConsumeMetrics(ctx, md); err != nil {
		if c.logger != nil {
			c.logger.Error("failed to consume metrics", zap.Error(err))
		}
		return err
	}

	return nil
}

// interval usato per il merge degli spans di uno stesso servizio
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
		return 0, nil, errors.New("no spans")
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
		return 0, nil, errors.New("invalid timestamps")
	}

	latencyMs := float64(maxEnd-minStart) / 1e6
	return latencyMs, serviceActiveNs, nil
}

// Opzionale, ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Opzionale, ridefinizione del metodo per chiudere il connector
func (c *connectorImp) Shutdown(ctx context.Context) error {
	return nil
}
*/

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

// Assunzione: upstream è presente groupbytrace, quindi td contiene gli spans della stessa trace.
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {

	latencyMs, serviceActiveNs, err := c.calculateE2ELatency(td, c.cfg)
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

	// Buckets bounds (ms)
	bounds := []float64{2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000}

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
		sumMs := float64(hs.sumNs) / 1e6
		buckets := append([]uint64(nil), hs.buckets...)
		hs.mu.Unlock()

		h := metric.SetEmptyHistogram()
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

		dp.ExplicitBounds().FromRaw(bounds)
		dp.SetCount(count)
		dp.SetSum(sumMs)
		dp.BucketCounts().FromRaw(buckets)

		dp.Attributes().PutStr("service_name", e2eKey)

	} else {
		g := metric.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(latencyMs + 100000)
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
			m.SetUnit("ms")

			if c.cfg.EnableHistogram {

				key := "service:" + svc

				// Osserva la latenza del servizio
				c.updateHistogram(key, activeNs, bounds)

				// Snapshot consistente dello stato cumulativo
				hs := c.getHistogramState(key, len(bounds)+1)
				hs.mu.Lock()
				count := hs.count
				sumMs := float64(hs.sumNs) / 1e6
				buckets := append([]uint64(nil), hs.buckets...)
				hs.mu.Unlock()

				h := m.SetEmptyHistogram()
				dp := h.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.Timestamp(time.Now().UnixNano()))

				dp.ExplicitBounds().FromRaw(bounds)
				dp.SetCount(count)
				dp.SetSum(sumMs)
				dp.BucketCounts().FromRaw(buckets)

				// Aggiunge una label alla metrica, con il nome del servizio
				dp.Attributes().PutStr("service_name", svc)

			} else {
				g := m.SetEmptyGauge()
				dp := g.DataPoints().AppendEmpty()
				dp.SetDoubleValue(svcLatencyMs)
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

	latencyMs := float64(latencyNs) / 1e6
	idx := len(bounds) // +Inf bucket
	// Controlla in quale bucket inserire la latenza
	for i, b := range bounds { // NB: OTLP/Prometheus cumulano automaticamente i buckets poi
		if latencyMs <= b {
			idx = i
			break
		}
	}
	hs.buckets[idx]++
}

// Opzionale, ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	return nil
}

// Opzionale, ridefinizione del metodo per chiudere il connector
func (c *connectorImp) Shutdown(ctx context.Context) error {
	return nil
}
