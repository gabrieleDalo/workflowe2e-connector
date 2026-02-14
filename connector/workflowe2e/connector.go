package workflowe2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

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

// Chiave composta per identificare un istogramma
type histKey struct {
	rootService      string
	rootOperation    string
	currentSvc       string // Usato per distinguere E2E (sarà "none") dai singoli servizi
	currentDeploy    string // Nome del deployment del servizio
	currentNamespace string // Nome del namespace del servizio
	experimentName   string // Nome dell'esperimento in corso
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
	mu                 sync.Mutex
	minStart           pcommon.Timestamp
	maxEnd             pcommon.Timestamp
	lastSeen           time.Time
	spanCount          int
	serviceIntervals   map[string][]interval // Lista per-service di intervalli non ancora mergiati
	serviceToDeploy    map[string]string     // Associa un servizio al corrispettivo Deployment (l'ultimo trovato in caso un servizio sia associato a più deployment)
	serviceToNamespace map[string]string     // Associa un servizio al corrispettivo namespace
	rootServiceName    string                // Il nome del servizio entry-point e l'operazione richieste identificano un workflow
	rootOperationName  string
	experimentName     string
	emitted            bool // Indica se abbiamo già emesso la metrica per questa trace (evita doppie emissioni)
}

// connectorImp implementa connector.Traces quindi, dobbiamo implementare:
// - component.Component (Start, Shutdown)
// - consumer.Traces (ConsumeTraces)
// Struttura del connector con i parametri desiderati
type connectorImp struct {
	metricsConsumer consumer.Metrics
	cfg             *Config
	logger          *zap.Logger

	db *sql.DB

	// Stato cumulativo degli histogram
	// permette di tenere lo stato memorizzato tra più chiamate a ConsumeTraces
	histMu    sync.RWMutex
	histState map[histKey]*histogramState // Ogni combinazione di label è una time series diversa

	// Stato per trace "incrementali" (se arrivano "spezzate")
	// permette di tenere lo stato memorizzato tra più chiamate a ConsumeTraces
	tracesMu sync.Mutex
	traces   map[string]*traceState

	// Parametri per timeout/flushing (impostabili via Config)
	traceIdleTimeout   time.Duration // tempo dopo il quale una trace viene considerata completa se non arrivano altri suoi spans
	traceFlushInterval time.Duration // frequenza di controllo
}

// createTracesToMetricsConnector della factory
// Funzione per creare un nuovo connector
func newConnector(
	_ context.Context,
	settings connector.Settings,
	cfg component.Config,
	metricsConsumer consumer.Metrics,
) (connector.Traces, error) {

	// type-assert della config
	myCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("Invalid config, expected *workflowe2e.Config")
	}

	// Inizializzo traces e default timeout
	return &connectorImp{
		metricsConsumer:    metricsConsumer,
		cfg:                myCfg,
		logger:             settings.TelemetrySettings.Logger,
		histState:          make(map[histKey]*histogramState),
		traces:             make(map[string]*traceState),
		traceIdleTimeout:   myCfg.TraceIdleTimeout,
		traceFlushInterval: myCfg.TraceFlushInterval,
		db:                 nil,
	}, nil
}

// Definisce le capacità del connector, in particolare se modifica i dati ricevuti o no
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Funzione helper per salvare i dati nel DB in modo asincrono (per non bloccare il resto del connector)
func (c *connectorImp) saveToDatabase(traceID string, experimentName string, rootService string, rootOperation string, startTs, endTs pcommon.Timestamp, latencyMs float64, svcLatencies map[string]float64) {

	// Se il DB non è configurato, esci
	if c.db == nil {
		return
	}

	// Goroutine per non bloccare il collector, il Collector non deve aspettare che Postgres risponda per processare la traccia successiva
	go func() {
		// Converte la mappa delle latenze per service in una stringa di byte in formato JSON
		jsonBytes, err := json.Marshal(svcLatencies)
		if err != nil {
			c.logger.Error("DB: JSON Marshal error", zap.Error(err))
			return
		}

		// Converte i Timestamp (ns) in Time.Time (UTC) per Postgres
		startTime := time.Unix(0, int64(startTs)).UTC()
		endTime := time.Unix(0, int64(endTs)).UTC()

		// Esegue Insert con timeout, crea un "contesto" che scade dopo n secondi
		// Se Postgres si blocca (es. deadlock o rete isolata), non vogliamo che migliaia di goroutine rimangano appese all'infinito consumando memoria
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// I Segnaposti ($1, $2...): Proteggono da SQL Injection (anche se qui i dati sono "puliti", è la prassi di sicurezza)
		query := `
            INSERT INTO trace_results (trace_id, experiment_name, rootService, rootOperation, start_time, end_time, latency_ms, services_latency)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (trace_id) DO NOTHING`

		// Invia la query al database usando il timeout definito sopra
		_, err = c.db.ExecContext(ctx, query, traceID, experimentName, rootService, rootOperation, startTime, endTime, latencyMs, jsonBytes)

		if err != nil {
			c.logger.Error("DB: Insert failed", zap.Error(err))
		}
	}()
}

// Utility per trasformare una map usata come insieme in una slice
// tipico quando vuoi prendere tutti gli elementi unici (es. trace IDs) e passarli a zap.Any, fmt.Println, iterare, ecc...
func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Funzione helper per estrarre l'operazione dagli spans di Istio (quelli di OTel hanno già operationName)
func (c *connectorImp) getCleanOperationName(span ptrace.Span) string {
	method, hasMethod := span.Attributes().Get("http.method")
	fullURL, hasURL := span.Attributes().Get("http.url")

	if hasMethod && hasURL {
		// Usa net/url per estrarre solo il path (es. /startProcess) escludento l'IP
		if parsed, err := url.Parse(fullURL.AsString()); err == nil {
			return method.AsString() + " " + parsed.Path
		}
	}

	if span.Name() != "" {
		return span.Name()
	}

	return "unknown"
}

// Funzione di discovery per il nome del servizio
func (c *connectorImp) getServiceName(span ptrace.Span, resource pcommon.Resource) string {
	// Se l'utente ha configurato un attributo specifico nelle config, viene usato come priorità assoluta
	if c.cfg.ServiceNameAttribute != "" {
		// Prima controllo se è negli attributi degli span (tipico di Istio)
		if v, ok := span.Attributes().Get(c.cfg.ServiceNameAttribute); ok {
			return v.AsString()
		}
		// Altrimenti controllo se è negli attributi delle resource (tipico di OTel)
		if v, ok := resource.Attributes().Get(c.cfg.ServiceNameAttribute); ok {
			return v.AsString()
		}
	}

	// Fallback su convenzioni standard (per Istio e OTel)
	if c.cfg.UsingIstio {
		// Cerchiamo nello span (tipico di Istio/Sidecar)
		spanKeys := []string{"istio.canonical_service", "peer.service"}
		for _, k := range spanKeys {
			if v, ok := span.Attributes().Get(k); ok && v.AsString() != "" {
				return v.AsString()
			}
		}
	} else {
		// Cerchiamo nella risorsa (tipico di SDK OTel)
		if v, ok := resource.Attributes().Get("service.name"); ok && v.AsString() != "" {
			return v.AsString()
		}
	}

	// Se non trova nulla
	return "unknown"
}

// Funzione di discovery per estrarre il nome del deployment
func (c *connectorImp) getDeploymentName(span ptrace.Span, resource pcommon.Resource) string {
	// Cerca negli attributi della risorsa (standard OTel)
	if v, ok := resource.Attributes().Get("k8s.deployment.name"); ok && v.AsString() != "" {
		return v.AsString()
	}
	// Altrimenti cerca negli attributi dello span
	if v, ok := span.Attributes().Get("k8s.deployment.name"); ok && v.AsString() != "" {
		return v.AsString()
	}
	// Cerco altri possibili nomi dell'attributo
	keys := []string{"deployment.name", "k8s.workload.name"}
	for _, k := range keys {
		if v, ok := resource.Attributes().Get(k); ok && v.AsString() != "" {
			return v.AsString()
		}
	}

	return "unknown"
}

func (c *connectorImp) getNamespaceName(span ptrace.Span, resource pcommon.Resource) string {
	// Cerca negli attributi della risorsa (standard OTel)
	if v, ok := resource.Attributes().Get("k8s.namespace.name"); ok && v.AsString() != "" {
		return v.AsString()
	}
	// Fallback su attributi dello span
	if v, ok := span.Attributes().Get("k8s.namespace.name"); ok && v.AsString() != "" {
		return v.AsString()
	}

	return "unknown"
}

// Funzione per verificare se uno span è generato da Istio
func (c *connectorImp) isIstioSidecar(span ptrace.Span) bool {
	// Controllo se lo span contiene attributi tipici di Istio
	if v, ok := span.Attributes().Get("component"); ok && v.AsString() == "proxy" {
		return true
	}
	// Oppure, Istio mette quasi sempre "istio.canonical_service"
	_, hasCanonical := span.Attributes().Get("istio.canonical_service")
	return hasCanonical
}

// Funzione che normalizza i nomi dei servizi (ad es. per Istio possono essere <nome servizio>.<nome namespace>)
func (c *connectorImp) normalizeServiceName(name string) string {
	if name == "" || name == "unknown" {
		return "unknown"
	}

	return strings.Split(name, ".")[0] // Se il nome è "s-0.default", prendiamo solo "s-0"
}

// Funzione che estrae il nome dell'esperimento dal rispettivo resource attribute, il baggage viene inserito come lista, perciò dobbiamo prendere il valore che ci interessa a noi
// Assumiamo che la chiave sia uguale al nome dell'attributo ma con un _ invece del .
// In alternativa, dovrei agggiungere un altro parametro di configurazione per specificare il nome della chiave da prendere dal baggage
func (c *connectorImp) getExperimentName(resource pcommon.Resource) string {

	attrKey := c.cfg.ExperimentNameAttribute
	headerKey := c.cfg.ExperimentNameHeader

	if attrKey == "" {
		attrKey = "none"
	}

	// Deriviamo la chiave interna del baggage (es: experiment_name)
	baggageKey := headerKey

	if v, ok := resource.Attributes().Get(attrKey); ok {
		raw := v.AsString()
		// Parsing della lista: ["chiave=valore"] -> valore
		// Cerchiamo il prefisso dinamico (es: ["experiment_name=)
		prefix := "[\"" + baggageKey + "="
		if strings.HasPrefix(raw, prefix) {
			clean := strings.TrimPrefix(raw, prefix)
			clean = strings.TrimSuffix(clean, "\"]")
			return clean
		}
		return raw // Fallback se non ha il formato lista
	}

	return "none"
}

// Funzione che aggiorna o crea lo stato per una trace quando riceve uno span
func (c *connectorImp) updateTraceStateForSpan(traceID string, start, end pcommon.Timestamp, svc string, deploy string, namespace string, opName string, expName string) {
	// Recupera o crea traceState
	c.tracesMu.Lock()
	ts := c.traces[traceID]
	if ts == nil {
		ts = &traceState{
			minStart:           maxTimestamp,
			maxEnd:             0,
			lastSeen:           time.Now(),
			serviceIntervals:   make(map[string][]interval),
			serviceToDeploy:    make(map[string]string),
			serviceToNamespace: make(map[string]string),
			experimentName:     expName,
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
			ts.rootServiceName = svc
			ts.rootOperationName = opName
		}
		if end > ts.maxEnd {
			ts.maxEnd = end
		}
		// Aggiunge l'intervallo per il servizio, se richiesto (dipende ad es. se vogliamo usare anche i tempi degli spans di Istio oppure no)
		if svc != "" && svc != "unknown" {
			ts.serviceIntervals[svc] = append(ts.serviceIntervals[svc], interval{start: start, end: end})

			// Salviamo anche il deployment del servizio, se lo troviamo
			// Se abbiamo già un nome (es. da uno span OTel), non lo sovrascriviamo con "unknown" (es. da uno span Istio che non ha il nome del deployment tra gli attributi)
			if deploy != "unknown" {
				ts.serviceToDeploy[svc] = deploy
			} else if _, exists := ts.serviceToDeploy[svc]; !exists {
				ts.serviceToDeploy[svc] = "unknown" // Inizializziamo a unknown solo se non ne abbiamo già uno associato al servizio
			}

			// Gestione Namespace (Nuova logica)
			if namespace != "unknown" {
				ts.serviceToNamespace[svc] = namespace
			} else if _, exists := ts.serviceToNamespace[svc]; !exists {
				ts.serviceToNamespace[svc] = "unknown"
			}
		}

		if (ts.experimentName == "" || ts.experimentName == "none") && (expName != "none" && expName != "") {
			ts.experimentName = expName
		}
	}
	// Aggiorna lastSeen sempre (anche per span aperti)
	ts.lastSeen = time.Now()
	ts.spanCount++
	ts.mu.Unlock()
}

// Restituisce lo stato cumulativo di un histogram (count, sum, buckets) identificato da una chiave, se lo stato non esiste ancora allora lo crea
func (c *connectorImp) getHistogramState(key histKey, bucketCount int) *histogramState {
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

	// Double-check locking: nel tempo tra RUnlock e Lock, un’altra goroutine potrebbe aver creato lo stato
	// Se ora lo stato esiste, lo restituisce senza ricrearlo
	if hs = c.histState[key]; hs != nil {
		return hs
	}

	now := pcommon.Timestamp(time.Now().UnixNano()) // Istante di creazione dell'istogramma (può essere utile per Prometheus)

	// Crea un nuovo histogramState vuoto: count e sumNs sono implicitamente 0.
	// buckets è una slice di lunghezza bucketCount, inizializzata a zero
	hs = &histogramState{
		buckets: make([]uint64, bucketCount),
		start:   now,
	}
	c.histState[key] = hs // Memorizza il nuovo stato nella mappa

	if c.logger != nil {
		c.logger.Debug("DEBUG_LOGS: created new histogramState",
			zap.String("key_rootService: ", key.rootService),
			zap.String("key_rootOperation: ", key.rootOperation),
			zap.String("key_currentSvc: ", key.currentSvc),
			zap.String("key_currentDeploy: ", key.currentDeploy),
			zap.Int("buckets_len", bucketCount),
			zap.Int64("start_ns", int64(now)),
		)
	}

	return hs
}

// Aggiorna lo stato cumulativo di un istogramma
func (c *connectorImp) updateHistogram(key histKey, latencyNs uint64, bounds []float64) {
	hs := c.getHistogramState(key, len(bounds)+1) // Recupera lo stato corrente dell’histogram per la chiave key. Se non esiste, lo crea. il +1 serve per il bound +inf

	// Aggiorno lo stato dell'istogramma
	hs.mu.Lock()
	defer hs.mu.Unlock()

	// Aggiorno count, sum e buckets dell'istogramma
	hs.count++
	hs.sumNs += latencyNs

	latencySec := float64(latencyNs) / 1e9 // Converto la latenza da nanosecondi a secondi
	idx := len(bounds)                     // +Inf bucket

	// Controlla in quale bucket inserire la latenza
	for i, b := range bounds { // NB: OTLP/Prometheus cumulano automaticamente i buckets
		if latencySec <= b {
			idx = i
			break
		}
	}
	hs.buckets[idx]++

	c.logger.Debug("DEBUG_LOGS: histogram update",
		zap.String("key_rootService: ", key.rootService),
		zap.String("key_rootOperation: ", key.rootOperation),
		zap.String("key_currentSvc: ", key.currentSvc),
		zap.String("key_currentDeploy: ", key.currentDeploy),
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

	// Altrimenit, salvo i valori da utilizzare per calcolare la latenza e la segno come emessa (per evitare che qualcun altro lo faccia nel frattempo)
	minStart := ts.minStart
	maxEnd := ts.maxEnd
	serviceIntervals := ts.serviceIntervals
	serviceToDeploy := ts.serviceToDeploy
	serviceToNamespace := ts.serviceToNamespace
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

	// Aggiorno l'istogramma cumulativo E2E
	rootSvc := ts.rootServiceName
	rootOp := ts.rootOperationName

	if rootSvc == "" {
		rootSvc = "unknown"
	}
	if rootOp == "" {
		rootOp = "unknown"
	}

	e2eKey := histKey{
		rootService:      rootSvc,
		rootOperation:    rootOp,
		currentSvc:       "none",
		currentDeploy:    "none",
		currentNamespace: "none",
		experimentName:   ts.experimentName,
	}
	c.updateHistogram(e2eKey, latencyNs, defaultBounds)

	// Creiamo una mappa per le latenza per servizio
	// Ci serve per convertirla in JSON per il db
	dbServiceLatencies := make(map[string]float64)

	// Se richiesto, faccio il merge degli intervalli per i singoli servizi e aggiorno l'istogramma
	if c.cfg.ServiceLatencyMode {
		for svc, ivs := range serviceIntervals {
			activeNs := mergeIntervals(ivs)
			if activeNs == 0 {
				continue
			}

			// Se non c'è il db non serve la mappa
			if c.db != nil {
				dbServiceLatencies[svc] = float64(activeNs) / 1e6
			}

			// Recuperiamo il deployment associato a questo servizio nella traccia
			deploy := serviceToDeploy[svc]
			if deploy == "" {
				deploy = "unknown"
			}

			namespace := serviceToNamespace[svc]
			if namespace == "" {
				namespace = "unknown"
			}

			// Creiamo una chiave composta per l'istogramma globale. Questo permette a Prometheus di avere serie distinte per ogni deployment
			// Formato: "service:nome-servizio|nome-deployment"
			keySvc := histKey{
				rootService:      rootSvc,
				rootOperation:    rootOp,
				currentSvc:       svc,
				currentDeploy:    deploy,
				currentNamespace: namespace,
				experimentName:   ts.experimentName,
			}
			c.updateHistogram(keySvc, activeNs, defaultBounds)
		}
	}

	// Salviamo i dati che ci interessano nel database
	c.saveToDatabase(traceID, ts.experimentName, rootSvc, rootOp, minStart, maxEnd, latencyMs, dbServiceLatencies)

	// NB: non emettiamo le metriche direttamente qui per evitare emissioni duplicate e problemi di timing con Prometheus scrapes
	// Aggiorniamo solo lo stato interno (histState) e lasciamo che il flusher periodico (emitHistSnapshot) esponga le metriche

	if c.logger != nil {
		c.logger.Debug("DEBUG_LOGS: finalizeTrace",
			zap.String("trace_id", traceID),
			zap.Float64("latency_ms", latencyMs),
		)
	}
}

// emitHistSnapshot costruisce un pmetric.Metrics snapshot a partire dallo stato cumulativo histState dell'istogramma e lo invia al consumer (una volta per snapshot)
func (c *connectorImp) emitHistSnapshot(ctx context.Context) {

	// Costruisco il Metrics payload
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()

	attrs := rm.Resource().Attributes()
	if c.cfg.UsingIstio {
		// Se usi Istio, marchiamo la metrica come prodotta dal monitoraggio mesh
		attrs.PutStr("service.name", "istio-workflow-monitor")
	} else {
		attrs.PutStr("service.name", "otel-workflow-monitor")
	}

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("workflowe2e-connector")

	// Come prima cosa, copiamo i riferimenti agli histogram sotto lock per evitare di trattenere il lock per troppo tempo
	c.histMu.RLock()
	if len(c.histState) == 0 {
		c.histMu.RUnlock()
		// niente da emettere
		return
	}
	// Copia superficiale delle chiavi e dei puntatori
	local := make(map[histKey]*histogramState, len(c.histState))
	for k, v := range c.histState {
		local[k] = v
	}
	c.histMu.RUnlock()

	// Per ogni histogram copia in locale anche lo stato (count,sum,buckets)
	for key, hs := range local {
		hs.mu.Lock()
		count := hs.count
		sumNs := hs.sumNs
		bucketsCopy := append([]uint64(nil), hs.buckets...)
		startTs := hs.start
		hs.mu.Unlock()

		// Appendo la metrica (histogram) al payload
		m := sm.Metrics().AppendEmpty()

		// Decido che nome di metrica usare
		if key.currentSvc == "none" {
			m.SetName(c.cfg.E2ELatencyMetricName)
			m.SetDescription("End-to-end workflow latency")
		} else {
			m.SetName(c.cfg.ServiceLatencyMetricName)
			m.SetDescription("Per-service latency (active time)")
		}

		m.SetUnit("s")

		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative) // Dichiaro esplicitamente che l'istogramma è cumulativo

		dp := h.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(startTs)                             // Settiamo lo StartTimestamp in modo che il prometheus-exporter possa esporre la serie come cumulativa
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now())) // Prometheus vede un "nuovo" datapoint poichè il timestamp cambia e quindi aggiorna la metrica
		dp.ExplicitBounds().FromRaw(defaultBounds)
		dp.SetCount(count)
		dp.SetSum(float64(sumNs) / 1e9) // somma in secondi
		dp.BucketCounts().FromRaw(bucketsCopy)

		dp.Attributes().PutStr("root_service", key.rootService)
		dp.Attributes().PutStr("root_operation", key.rootOperation)
		dp.Attributes().PutStr("experiment_name", key.experimentName)

		if key.currentSvc != "none" {
			dp.Attributes().PutStr("generated_from_service", key.currentSvc)
			dp.Attributes().PutStr("service_deployment_name", key.currentDeploy)
			dp.Attributes().PutStr("service_deployment_namespace", key.currentNamespace)
		}
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

	// Aggiorno lo stato, per ogni span
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)

		experimentName := c.getExperimentName(rs.Resource())

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ { // Ciclo sugli spans delle trace
				span := spans.At(k)
				tid := span.TraceID().String() // Controllo i trace_id dagli spans, per capire quali traces diverse sono arrivate insieme
				traceIDs[tid] = struct{}{}

				isIstioSpan := c.isIstioSidecar(span) // Verifichiamo se è uno span di Istio
				// Se UsingIstio è false ed è uno span Istio allora non viene considerato, in tutti gli altri casi si
				if !(!c.cfg.UsingIstio && isIstioSpan) {

					start := span.StartTimestamp()
					end := span.EndTimestamp()

					svcToUpdate := ""
					deployName := ""
					opName := ""
					namespaceName := ""

					if c.cfg.UsingIstio && isIstioSpan {
						opName = c.getCleanOperationName(span)
					} else if !c.cfg.UsingIstio && !isIstioSpan {
						opName = span.Name()
					}

					if c.cfg.ServiceLatencyMode {
						// Determina il nome del servizio (controllando vari attributi a seconda se stiamo usando Istio o meno)
						rawSvcName := c.getServiceName(span, rs.Resource())
						svcToUpdate = c.normalizeServiceName(rawSvcName)
						deployName = c.getDeploymentName(span, rs.Resource())
						namespaceName = c.getNamespaceName(span, rs.Resource())
					}

					// Aggiorno lo stato per la trace
					c.updateTraceStateForSpan(tid, start, end, svcToUpdate, deployName, namespaceName, opName, experimentName)
				}
			}
		}
	}

	c.logger.Debug("DEBUG_LOGS: trace ids summary",
		zap.Int("unique_trace_count", len(traceIDs)),
		zap.Any("trace_ids", keys(traceIDs)),
	)

	return nil
}

// Ridefinizione del metodo per avviare il connector
func (c *connectorImp) Start(ctx context.Context, host component.Host) error {
	if c.logger != nil {
		c.logger.Info("DEBUG_LOGS: connector starting",
			zap.String("metric_name_e2e", c.cfg.E2ELatencyMetricName),
		)
	}

	// Se configurato, mi connetto al db
	if c.cfg.DBURL != "" {
		db, err := sql.Open("postgres", c.cfg.DBURL)
		if err != nil {
			c.logger.Error("DB: Could not open connection", zap.Error(err))
		} else {
			// Configuriamo il pool di connessioni (opzionale ma consigliato)
			db.SetMaxOpenConns(10)
			db.SetMaxIdleConns(5)
			c.db = db
			c.logger.Info("DB: Connection pool initialized")
		}
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

				// Dopo avere finalizzato traces inattive, emetto lo stato degli histogram cumulativi
				// Devo emetterlo continuamente in modo che Prometheus possa fare lo scrape
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
	// Al momento dello shutdown finalizza prima tutte le traces rimaste in sospeso
	c.tracesMu.Lock()
	var all []string
	for tid := range c.traces {
		all = append(all, tid)
	}
	c.tracesMu.Unlock()

	for _, tid := range all {
		c.finalizeTrace(tid)
	}

	c.emitHistSnapshot(context.Background())

	// Se era presente, termino la connessione al db
	if c.db != nil {
		c.logger.Info("DB: Closing database connection...")
		c.db.Close()
	}

	return nil
}
