package workflowe2e

import (
	"context"
	"errors"
	"sort"
	"strings"
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

// Bounds dei buckets (in secondi, coerente per Prometheus)
// centralizziamo i bounds così tutte le funzioni usano lo stesso set
var defaultBounds = []float64{0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.4, 2.0, 5.0, 10.0, 15.0}

// Stato cumulativo di un histogram
// NB: lo stato è in memoria del processo. Se il collector si riavvia lo stato viene perso e gli histogram ripartono da zero
type histogramState struct {
	mu      sync.Mutex        // mutex per thread-safety
	count   uint64            // totale osservazioni
	sumNs   uint64            // totale latenza
	buckets []uint64          // buckets cumulativi
	start   pcommon.Timestamp // start timestamp della serie cumulativa (impostato alla creazione)
}

// Intervallo usato per il merge degli spans di uno stesso servizio
// Contiene il tempo di inizio e fine per uno span
// Serve per gestire il caso in cui un servizio esegue, poi si passa ad un altro servizio e poi si torna indietro
type interval struct {
	start pcommon.Timestamp
	end   pcommon.Timestamp
}

// Stato temporaneo per trace parziali (per supportare trace "spezzate") (causate spesso da un workflow asincrono)
// non è assicurato che tutti gli spans di una traccia arrivino insieme
// NB: lo stato è in memoria del processo. Se il collector si riavvia lo stato viene perso
type traceState struct {
	mu               sync.Mutex
	minStart         pcommon.Timestamp
	maxEnd           pcommon.Timestamp
	lastSeen         time.Time
	spanCount        int
	serviceIntervals map[string][]interval // per-service lista di intervalli non ancora mergiati
	emitted          bool                  // Indica se abbiamo già emesso la metrica per questa trace (evita doppie emissioni)
}

// connectorImp implementa connector.Traces quindi, dobbiamo implementare:
// - component.Component (Start, Shutdown)
// - consumer.Traces (ConsumeTraces)
// Struttura del connector con i parametri desiderati
type connectorImp struct {
	metricsConsumer consumer.Metrics
	cfg             *Config
	logger          *zap.Logger

	// Stato cumulativo degli histogram
	// permette di tenere lo stato memorizzato tra più chiamate a ConsumeTraces
	histMu    sync.RWMutex
	histState map[string]*histogramState // Ogni combinazione di label è una time series diversa

	// Stato per trace "incrementali" (se arrivano "spezzate")
	// permette di tenere lo stato memorizzato tra più chiamate a ConsumeTraces
	tracesMu sync.Mutex
	traces   map[string]*traceState

	// Parametri per timeout/flushing (impostabili via Config)
	traceIdleTimeout   time.Duration // tempo dopo il quale una trace viene considerata completa se non arrivano altri suoi spans
	traceFlushInterval time.Duration // frequenza di controllo

	// Includi anche i seguenti parametri se non vuoi un'implementazione specifica per i metodi Start e Shutdown e togli i metodi alla fine del codice
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

	// Inizializzo traces e default timeout (puoi esporre questi in Config se vuoi)
	return &connectorImp{
		metricsConsumer:    metricsConsumer,
		cfg:                myCfg,
		logger:             settings.TelemetrySettings.Logger,
		histState:          make(map[string]*histogramState),
		traces:             make(map[string]*traceState),
		traceIdleTimeout:   myCfg.TraceIdleTimeout,
		traceFlushInterval: myCfg.TraceFlushInterval,
	}, nil
}

// Definisce le capacità del connector, in particolare se modifica i dati ricevuti o no
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Utility per trasformare una map usata come set in una slice
// tipico quando vuoi prendere tutti gli elementi unici (es. trace IDs) e passarli a zap.Any, fmt.Println, iterare, ecc...
func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Funzione che aggiorna o crea lo stato per una trace_id quando riceve uno span
func (c *connectorImp) updateTraceStateForSpan(traceID string, start, end pcommon.Timestamp, svc string) {
	// Recupera o crea traceState
	c.tracesMu.Lock()
	ts := c.traces[traceID]
	if ts == nil {
		ts = &traceState{
			minStart:         maxTimestamp,
			maxEnd:           0,
			lastSeen:         time.Now(),
			serviceIntervals: make(map[string][]interval),
		}
		c.traces[traceID] = ts
	}
	c.tracesMu.Unlock()

	// Aggiornamento dei tempi
	ts.mu.Lock()
	// Aggiorna min/max se end != 0 (span terminato)
	if end != 0 {
		if start < ts.minStart {
			ts.minStart = start
		}
		if end > ts.maxEnd {
			ts.maxEnd = end
		}
		// Aggiunge l'intervallo per il servizio (solo se abbiamo end)
		if svc == "" {
			svc = "unknown"
		}
		ts.serviceIntervals[svc] = append(ts.serviceIntervals[svc], interval{start: start, end: end})
	}
	// Aggiorna lastSeen sempre (anche per span aperti)
	ts.lastSeen = time.Now()
	ts.spanCount++
	ts.mu.Unlock()
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

	now := pcommon.Timestamp(time.Now().UnixNano())

	// Crea un nuovo histogramState vuoto: count e sumNs sono implicitamente 0.
	// buckets è una slice di lunghezza bucketCount, inizializzata a zero
	hs = &histogramState{
		buckets: make([]uint64, bucketCount),
		start:   now,
	}
	c.histState[key] = hs // Memorizza il nuovo stato nella mappa

	if c.logger != nil {
		c.logger.Debug("DEBUG_LOGS: created new histogramState",
			zap.String("key", key),
			zap.Int("buckets_len", bucketCount),
			zap.Int64("start_ns", int64(now)),
		)
	}

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

// mergeIntervals calcola la durata totale (ns) a partire da una slice di interval unmerged
// Restituisce il tempo effettivo di attività (considerando intervalli sovrapposti o tempi di idle)
func mergeIntervals(iv []interval) uint64 {
	if len(iv) == 0 {
		return 0
	}
	// Ordina gli intervalli di uno span sulla base dell'istante di inizio
	sort.Slice(iv, func(i, j int) bool {
		return iv[i].start < iv[j].start
	})
	cur := iv[0] // Partiamo dal primo intervallo
	var total uint64
	// Loop per il merge, prende ogni intervallo successivo e lo confronti con cur
	for i := 1; i < len(iv); i++ {
		// Controlla se l'intervallo inizia mentre uno è ancora attivo (lo span non è terminato)
		if iv[i].start <= cur.end {
			if iv[i].end > cur.end { // Se lo è, controlla se finisce dopo
				cur.end = iv[i].end // In tal caso aggiorno il tempo di fine
			}
		} else { // Se no, salvo il tempo trascorso in attività e passo al nuovo intervallo
			total += uint64(cur.end - cur.start)
			cur = iv[i]
		}
	}
	total += uint64(cur.end - cur.start)

	return total
}

// Finalizza una trace: calcola latenza e la emette come metrica (usando l'istogramma cumulativo)
// Centralizza l'emissione delle metriche e garantisce che ogni trace venga emessa una sola volta, usando il campo emitted nella traceState
func (c *connectorImp) finalizeTrace(traceID string) {
	// Recupera e rimuove lo stato della trace dalla mappa
	c.tracesMu.Lock()
	ts := c.traces[traceID]
	if ts == nil {
		c.tracesMu.Unlock()
		return
	}

	// Controlla se è già stata emessa e in caso la rimuove dalla mappa
	ts.mu.Lock()
	if ts.emitted {
		ts.mu.Unlock()
		delete(c.traces, traceID)
		c.tracesMu.Unlock()
		return
	}

	// Salvo i valori da utilizzare per calcolare la latenza e la segno come emessa (per evitare che qualcun altro lo faccia)
	minStart := ts.minStart
	maxEnd := ts.maxEnd
	serviceIntervals := ts.serviceIntervals
	ts.emitted = true
	ts.mu.Unlock()

	// Rimuovo la trace dallo stato globale
	delete(c.traces, traceID)
	c.tracesMu.Unlock()

	// Controllo la validità dei dati
	if minStart == maxTimestamp || maxEnd == 0 || maxEnd <= minStart {
		// Dati non validi per calcolo latenza: ignoro (ma loggo)
		if c.logger != nil {
			c.logger.Debug("DEBUG_LOGS: finalizeTrace skipped, invalid timestamps", zap.String("trace_id", traceID))
		}
		return
	}

	// Calcolo la latenza
	latencyNs := uint64(maxEnd - minStart)
	latencyMs := float64(latencyNs) / 1e6

	// Aggiorno l'istogramma cumulativo E2E (stato in memoria)
	const e2eKey = "__e2e__"
	c.updateHistogram(e2eKey, latencyNs, defaultBounds)

	// Se richiesto, Faccio il merge degli intervalli per i singoli servizi e aggiorno l'istogramma (stato in memoria)
	if c.cfg.ServiceLatencyMode != "none" {
		for svc, ivs := range serviceIntervals {
			activeNs := mergeIntervals(ivs)
			if activeNs == 0 {
				continue
			}
			key := "service:" + svc
			c.updateHistogram(key, activeNs, defaultBounds)
		}
	}

	// NOTA: non emettiamo più le metriche direttamente qui con ConsumeMetrics per evitare emissioni duplicate
	// e problemi di timing con Prometheus scrapes. Aggiorniamo solo lo stato interno (histState) e lasciamo
	// che il flusher periodico (emitHistSnapshot) esponga lo snapshot coerente.
	if c.logger != nil {
		c.logger.Debug("DEBUG_LOGS: finalizeTrace",
			zap.String("trace_id", traceID),
			zap.Float64("latency_ms", latencyMs),
		)
	}
}

// emitHistSnapshot costruisce un pmetric.Metrics snapshot a partire dallo stato cumulativo histState
// e lo invia al consumer (una volta per snapshot).
// NON imposta timestamp espliciti sui datapoint (lascia che l'exporter / Prometheus handle lo scrape time).
func (c *connectorImp) emitHistSnapshot(ctx context.Context) {

	// Costruisco il Metrics payload
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()

	rm.Resource().Attributes().PutStr("service.name", "otelcol-workflowe2e") // DA MODIFICARE, A SECONDA DI UsingIstio

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("workflowe2e-connector")

	// Primo: copia i riferimenti agli histogram sotto lock per evitare holding prolungati della map
	c.histMu.RLock()
	if len(c.histState) == 0 {
		c.histMu.RUnlock()
		// niente da emettere
		return
	}
	// copia shallow delle chiavi e dei puntatori
	local := make(map[string]*histogramState, len(c.histState))
	for k, v := range c.histState {
		local[k] = v
	}
	c.histMu.RUnlock()

	// Per ogni histogram copia in locale lo stato (count,sum,buckets) sotto hs.mu
	for key, hs := range local {
		hs.mu.Lock()
		count := hs.count
		sumNs := hs.sumNs
		bucketsCopy := append([]uint64(nil), hs.buckets...)
		startTs := hs.start
		hs.mu.Unlock()

		// Decido che nome di metrica usare
		var metricName string
		attrsSvc := ""
		if key == "__e2e__" {
			metricName = c.cfg.E2ELatencyMetricName
			attrsSvc = "__e2e__"
		} else if strings.HasPrefix(key, "service:") {
			metricName = c.cfg.ServiceLatencyMetricName
			attrsSvc = strings.TrimPrefix(key, "service:")
		} else {
			metricName = c.cfg.E2ELatencyMetricName
			attrsSvc = key
		}

		// Appendo la metrica (histogram) al payload
		m := sm.Metrics().AppendEmpty()
		m.SetName(metricName)
		if metricName == c.cfg.E2ELatencyMetricName {
			m.SetDescription("End-to-end workflow latency")
		} else {
			m.SetDescription("Per-service latency (active time)")
		}
		m.SetUnit("s")

		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative) // Dichiaro esplicitamente che è cumulativo
		dp := h.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(startTs)                             // Settiamo lo StartTimestamp in modo che il prometheus-exporter possa esporre la serie come cumulativa
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now())) // Aggiungi questa riga per forzare Prometheus a vedere un "nuovo" datapoint
		dp.ExplicitBounds().FromRaw(defaultBounds)
		dp.SetCount(count)
		dp.SetSum(float64(sumNs) / 1e9) // somma in secondi
		dp.BucketCounts().FromRaw(bucketsCopy)
		dp.Attributes().PutStr("service_name", attrsSvc)
		//dp.Attributes().PutInt("debug_tick", time.Now().Unix()) // Con questo funziona correttamente ma vengono create esponenzialmente metriche nuove poichè cambia la label tick
	}

	// Emetto lo snapshot in un'unica chiamata
	if err := c.metricsConsumer.ConsumeMetrics(ctx, md); err != nil {
		if c.logger != nil {
			c.logger.Error("Failed to emit histogram snapshot", zap.Error(err))
		}
	} else {
		if c.logger != nil {
			c.logger.Debug("DEBUG_LOGS: emitHistSnapshot emitted histogram snapshot",
				zap.Int("series", len(local)))
		}
	}
}

// Contiene la logica vera e propria per utilizzare le traces che arrivano al connector
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {

	c.logger.Debug("DEBUG_LOGS: ConsumeTraces called",
		zap.Int("span_count", td.SpanCount()),
	)

	// Come prima cosa raccolgo info per ogni trace presente nel batch
	traceIDs := make(map[string]struct{})
	//preExisting := make(map[string]bool)   // Usata per sapere se una trace era già esistente nello stato prima di questo batch
	//batchAllEnded := make(map[string]bool) // Usata per sapere se tutte le span del batch per una trace sono terminate (hanno endTime != 0)

	// Segno quali traces esistono già (hanno già uno stato)
	//c.tracesMu.Lock()
	//for tid := range c.traces {
	//	preExisting[tid] = true
	//}
	//c.tracesMu.Unlock()

	// Aggiorno lo stato per ogni span e raccolgo se nel batch
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				tid := span.TraceID().String() // Controllo i trace_id dagli spans, per capire quali traces diverse sono arrivate insieme
				traceIDs[tid] = struct{}{}

				start := span.StartTimestamp()
				end := span.EndTimestamp()

				// determina il nome del servizio (stessa logica del calcolo successivo)
				var svc string
				// prefer span attribute (istio mode) altrimenti resource attr
				if c.cfg.UsingIstio {
					if v, ok := span.Attributes().Get(c.cfg.ServiceNameAttribute); ok {
						svc = v.AsString()
					}
				}
				if svc == "" {
					if v, ok := rs.Resource().Attributes().Get(c.cfg.ServiceNameAttribute); ok {
						svc = v.AsString()
					}
				}
				if svc == "" {
					svc = "unknown"
				}

				// Aggiorno lo stato per la trace
				c.updateTraceStateForSpan(tid, start, end, svc)
			}
		}
	}

	c.logger.Debug("DEBUG_LOGS: trace ids summary",
		zap.Int("unique_trace_count", len(traceIDs)),
		zap.Any("trace_ids", keys(traceIDs)),
	)

	// NOTE: qui NON emetto metriche per traces parziali; il flusher in Start() si occuperà di
	// finalizzare eventuali traces che restano inert per traceIdleTimeout.

	return nil
}

// Ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	if c.logger != nil {
		c.logger.Info("DEBUG_LOGS: connector starting",
			zap.String("metric_name_e2e", c.cfg.E2ELatencyMetricName),
		)
	}

	// Lancia la goroutine di flush periodico per finalizzare le traces inattive (esegue in background)
	// Ogni traceFlushInterval il flusher controlla tutte le trace e, per ognuna, verifica se è passato più di traceIdleTimeout dall’ultima volta che ha visto uno span di quella trace. Se sì → la finalizza
	// Inoltre ad ogni tick emettiamo lo snapshot (emitHistSnapshot) degli histogram cumulativi.
	go func() {
		// Crea un Ticker che invia un evento sul canale ticker.C ogni traceFlushInterval. È il meccanismo per eseguire periodicamente un compito (in questo caso il flush)
		ticker := time.NewTicker(c.traceFlushInterval)
		defer ticker.Stop() // Garantisce che al termine della goroutine il ticker venga fermato
		// Loop infinito che aspetta eventi concorrenti. select è il costrutto Go per aspettare su più canali simultaneamente
		for {
			select {
			// Succede ogni volta che il ticker "scatta" (cioè ogni traceFlushInterval). Dentro questo case avviene il lavoro periodico di controllo delle trace inattive
			case <-ticker.C:
				cutoff := time.Now().Add(-c.traceIdleTimeout) // Calcola il momento temporale prima del quale consideriamo una trace inattiva. Se lastSeen di una trace è prima di cutoff, la trace è ferma da più di traceIdleTimeout

				// Prelevo le trace da finalizzare
				c.tracesMu.Lock()
				var toFinalize []string
				// NB: in caso di molte traces potrebbe essere consigliabile usare il mutex con dentro il for per evitare lock e unlock continui
				for tid, ts := range c.traces {
					ts.mu.Lock()
					last := ts.lastSeen
					emitted := ts.emitted
					ts.mu.Unlock()
					// Se è già stata emessa, verrà rimossa dalla finalizeTrace, ma possiamo segnalarlo
					if emitted {
						toFinalize = append(toFinalize, tid)
						continue
					}
					// Se l'ultima volta che abbiamo visto un evento per questa trace è prima del cutoff (cioè è passato più di traceIdleTimeout), la consideriamo terminata e la mettiamo nella lista toFinalize
					if !last.IsZero() && last.Before(cutoff) {
						toFinalize = append(toFinalize, tid)
					}
				}
				c.tracesMu.Unlock()

				// Per ogni trace da finalizzare chiamo la funzione di finalizzazione
				for _, tid := range toFinalize {
					c.finalizeTrace(tid)
				}

				// Dopo avere finalizzato traces inattive, emetto uno snapshot coerente degli histogram cumulativi
				c.emitHistSnapshot(context.Background())
			// Quando il Collector chiama Shutdown (o il contesto viene cancellato), la goroutine esce in modo pulito
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Ridefinizione del metodo per chiudere il connector
func (c *connectorImp) Shutdown(ctx context.Context) error {
	// Al momento dello shutdown potresti voler flushare tutte le trace rimaste
	c.tracesMu.Lock()
	var all []string
	for tid := range c.traces {
		all = append(all, tid)
	}
	c.tracesMu.Unlock()

	for _, tid := range all {
		c.finalizeTrace(tid)
	}
	// Emitto uno snapshot finale
	c.emitHistSnapshot(context.Background())
	return nil
}
