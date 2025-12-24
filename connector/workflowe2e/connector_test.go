package workflowe2e

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Questa è una struct “fake”, serve per catturare le metriche generate dal connector
// Invece di inviarle a Prometheus o ad un exporter reale, le memorizza in memoria (metrics []pmetric.Metrics) per permetterci di verificarle nei test
// (implementa consumer.Metrics per catturare le metriche generate)
type fakeMetricsConsumer struct {
	metrics []pmetric.Metrics
}

// Implementa l’interfaccia consumer.Metrics, implementanto i seguenti 2 metodi
// ConsumeMetrics è quello che il connector chiama quando genera metriche
// Qui le appendiamo semplicemente alla slice metrics per verificarle dopo
func (f *fakeMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (f *fakeMetricsConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	f.metrics = append(f.metrics, md)
	return nil
}

// Testiamo che il calcolo della latenza e2e dell'intero workflow funzioni se esponiamo la metrica come gauge
func TestConnectorWorkflowE2ELatencyGauge(t *testing.T) {
	// Forniamo le configurazioni per il test, nella realtà vengono fornite nel config.yaml del collector e passate automaticamente
	// Configuriamo il connector per usare Gauge (EnableHistogram: false) e senza metriche per singoli servizi (ServiceLatencyMode: "none"
	cfg := &Config{
		E2ELatencyMetricName:     "workflow_e2e_latency",
		ServiceLatencyMetricName: "workflow_service_latency",
		ServiceLatencyMode:       false,
		ServiceNameAttribute:     "service.name",
		UsingIstio:               false, // solo OTel
	}

	// Creiamo il “fake sink” per catturare le metriche
	// E iniettiamo il sink e la config nel connector
	sink := &fakeMetricsConsumer{}

	c := &connectorImp{
		metricsConsumer: sink,
		cfg:             cfg,
		histState:       make(map[string]*histogramState),
	}

	// Creiamo una trace fittizia con un solo span.
	// Lo span ha timestamp di inizio e fine impostati a 120ms fa → ora
	// Serve per testare che il connector calcoli la latenza E2E correttamente
	var latencyToTest float64 = 120.0 // Latenza che deve risultare
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	start := time.Now().Add(time.Duration(latencyToTest) * time.Millisecond * (-1))
	end := time.Now()
	span.SetStartTimestamp(pcommon.Timestamp(start.UnixNano()))
	span.SetEndTimestamp(pcommon.Timestamp(end.UnixNano()))

	// Chiamiamo il connector sulla trace fittizia.
	// Se fallisce, il test termina con Fatal
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces failed: %v", err)
	}

	// Controlliamo che il connector abbia effettivamente generato almeno una metrica
	if len(sink.metrics) == 0 {
		t.Fatal("No metrics emitted")
	}

	// Estraiamo la metrica E2E dal Metrics tree gerarchico (ResourceMetrics → ScopeMetrics → Metrics)
	// Controlliamo che il nome sia corretto.
	md := sink.metrics[0]
	metric := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if metric.Name() != cfg.E2ELatencyMetricName {
		t.Errorf("Metric name mismatch, got %s", metric.Name())
	}

	// Controlliamo che il gauge abbia un solo datapoint, come previsto
	g := metric.Gauge()
	if g.DataPoints().Len() != 1 {
		t.Errorf("Expected 1 datapoint, got %d", g.DataPoints().Len())
	}

	// Controlliamo che il valore della latenza sia positivo
	dp := g.DataPoints().At(0)
	latency := dp.DoubleValue()
	if latency <= 0 {
		t.Errorf("Expected positive latency, got %f", latency)
	}

	// Controlliamo che il valore della latenza calcolato dal connector sia quello che ci aspettiamo, con una certa tolleranza
	epsilon := 0.01 // NB: i float in Go spesso non sono rappresentati precisamente, è meglio usare un epsilon quando si fanno test di uguaglianza tra float
	if !(latencyToTest-epsilon <= latency && latency <= latencyToTest+epsilon) {
		t.Errorf("Expected different latency, got %f", latency)
	} else {
		t.Logf("Correct, got latency expected: %f", latency)

		/*for _, v := range dp.Attributes().All() {
			t.Logf("Map: %s", v.AsString())
		}*/
	}
}

func TestConnectorWorkflowE2ELatencyHistogramCumulative(t *testing.T) {
	cfg := &Config{
		E2ELatencyMetricName:     "workflow_e2e_latency_ms",
		ServiceLatencyMetricName: "workflow_service_latency_ms",
		ServiceLatencyMode:       false,
		ServiceNameAttribute:     "service.name",
		UsingIstio:               false,
	}

	sink := &fakeMetricsConsumer{}

	c := &connectorImp{
		metricsConsumer: sink,
		cfg:             cfg,
		histState:       make(map[string]*histogramState),
	}

	bounds := []float64{2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10_000, 15_000}

	// Latency di test in ms
	latencies := []float64{120.0, 250.0, 75.0}

	for _, latencyMs := range latencies {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		ss := rs.ScopeSpans().AppendEmpty()
		span := ss.Spans().AppendEmpty()
		start := time.Now().Add(time.Duration(-latencyMs) * time.Millisecond)
		end := time.Now()
		span.SetStartTimestamp(pcommon.Timestamp(start.UnixNano()))
		span.SetEndTimestamp(pcommon.Timestamp(end.UnixNano()))

		if err := c.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatalf("ConsumeTraces failed: %v", err)
		}
	}

	// Ora controlliamo lo stato cumulativo direttamente
	hs := c.getHistogramState("__e2e__", len(bounds)+1)
	hs.mu.Lock()
	count := hs.count
	sumMs := float64(hs.sumNs) / 1e6
	buckets := append([]uint64(nil), hs.buckets...)
	hs.mu.Unlock()

	// Count cumulativo deve essere 3
	if count != uint64(len(latencies)) {
		t.Fatalf("Expected count=%d, got %d", len(latencies), count)
	} else {
		t.Logf("Correct, got count expected: %d", count)
	}

	// Sum cumulativo deve essere la somma delle latenze
	var expectedSum float64
	for _, v := range latencies {
		expectedSum += v
	}
	epsilon := 0.01
	if !(expectedSum-epsilon <= sumMs && sumMs <= expectedSum+epsilon) {
		t.Fatalf("Unexpected sum: got %f, want ~%f", sumMs, expectedSum)
	} else {
		t.Logf("Correct, got sum expected: %f", sumMs)
	}

	// Verifica che i bucket siano aggiornati cumulativamente
	expectedBuckets := make([]uint64, len(bounds)+1)
	for _, v := range latencies {
		idx := len(bounds)
		for i, b := range bounds {
			if v <= b {
				idx = i
				break
			}
		}
		expectedBuckets[idx]++
	}

	for i, v := range buckets {
		if v != expectedBuckets[i] {
			t.Fatalf("Expected buckets[%d]=%d, got %d", i, expectedBuckets[i], v)
		} else {
			t.Logf("Correct, got buckets expected, buckets[%d]=%d", i, v)
		}
	}
}

/*
func TestConnectorE2ELatencyHistogram(t *testing.T) {
	cfg := &Config{
		E2ELatencyMetricName:     "workflow_e2e_latency_ms",
		ServiceLatencyMetricName: "workflow_service_latency_ms",
		ServiceLatencyMode:       "all",
		ServiceNameAttribute:     "service.name",
		EnableHistogram:          true, // histogram
		UsingIstio:               false,
	}

	sink := &fakeMetricsConsumer{}

	c := &connectorImp{
		metricsConsumer: sink,
		cfg:             cfg,
	}

	td := ptrace.NewTraces()

	// ResourceSpans per svc1
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "svc1")
	ss1 := rs1.ScopeSpans().AppendEmpty()
	span1 := ss1.Spans().AppendEmpty()
	start1 := time.Now().Add(-200 * time.Millisecond)
	end1 := time.Now().Add(-100 * time.Millisecond)
	span1.SetStartTimestamp(pcommon.Timestamp(start1.UnixNano()))
	span1.SetEndTimestamp(pcommon.Timestamp(end1.UnixNano()))

	// ResourceSpans per svc2
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "svc2")
	ss2 := rs2.ScopeSpans().AppendEmpty()
	span2 := ss2.Spans().AppendEmpty()
	start2 := time.Now().Add(-150 * time.Millisecond)
	end2 := time.Now()
	span2.SetStartTimestamp(pcommon.Timestamp(start2.UnixNano()))
	span2.SetEndTimestamp(pcommon.Timestamp(end2.UnixNano()))

	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces failed: %v", err)
	}

	if len(sink.metrics) == 0 {
		t.Fatal("no metrics emitted")
	}

	// Verifica E2E metric
	e2eMetric := sink.metrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if e2eMetric.Name() != cfg.E2ELatencyMetricName {
		t.Errorf("E2E metric name mismatch, got %s", e2eMetric.Name())
	}
	hist := e2eMetric.Histogram()
	if hist.DataPoints().Len() != 1 {
		t.Errorf("expected 1 histogram datapoint, got %d", hist.DataPoints().Len())
	}

	// Verifica metriche per servizi
	metrics := sink.metrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	foundSvc1 := false
	foundSvc2 := false
	for i := 1; i < metrics.Len(); i++ {
		m := metrics.At(i)
		dp := m.Histogram().DataPoints().At(0)
		val, ok := dp.Attributes().Get("service")
		service := ""
		if ok {
			service = val.AsString()
		}

		if service == "svc1" {
			foundSvc1 = true
		} else if service == "svc2" {
			foundSvc2 = true
		}
	}

	if !foundSvc1 || !foundSvc2 {
		t.Error("service metrics not found for all expected services")
	}
}
*/
