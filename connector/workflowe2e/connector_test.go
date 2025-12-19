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
		E2ELatencyMetricName:     "workflow_e2e_latency_ms",
		ServiceLatencyMetricName: "workflow_service_latency_ms",
		ServiceLatencyMode:       "none",
		ServiceNameAttribute:     "service.name",
		EnableHistogram:          false, // gauge
		UsingIstio:               false, // solo OTel
	}

	// Creiamo il “fake sink” per catturare le metriche
	// E iniettiamo il sink e la config nel connector
	sink := &fakeMetricsConsumer{}

	c := &connectorImp{
		metricsConsumer: sink,
		cfg:             cfg,
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
		t.Fatal("no metrics emitted")
	}

	// Estraiamo la metrica E2E dal Metrics tree gerarchico (ResourceMetrics → ScopeMetrics → Metrics)
	// Controlliamo che il nome sia corretto.
	md := sink.metrics[0]
	metric := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if metric.Name() != cfg.E2ELatencyMetricName {
		t.Errorf("metric name mismatch, got %s", metric.Name())
	}

	// Controlliamo che il gauge abbia un solo datapoint, come previsto
	g := metric.Gauge()
	if g.DataPoints().Len() != 1 {
		t.Errorf("expected 1 datapoint, got %d", g.DataPoints().Len())
	}

	// Controlliamo che il valore della latenza sia positivo
	dp := g.DataPoints().At(0)
	latency := dp.DoubleValue()
	if latency <= 0 {
		t.Errorf("expected positive latency, got %f", latency)
	}

	// Controlliamo che il valore della latenza calcolato dal connector sia quello che ci aspettiamo, con una certa tolleranza
	epsilon := 0.001 // NB: i float in Go spesso non sono rappresentati precisamente, è meglio usare un epsilon quando si fanno test di uguaglianza tra float
	if !(latencyToTest-epsilon <= latency && latency <= latencyToTest+epsilon) {
		t.Errorf("expected different latency, got %f", latency)
	}
}

func TestConnectorWorkflowE2ELatencyHistogram(t *testing.T) {

	cfg := &Config{
		E2ELatencyMetricName:     "workflow_e2e_latency_ms",
		ServiceLatencyMetricName: "workflow_service_latency_ms",
		ServiceLatencyMode:       "none", // non vogliamo metriche per servizio qui
		ServiceNameAttribute:     "service.name",
		EnableHistogram:          true, // histogram
		UsingIstio:               false,
	}

	sink := &fakeMetricsConsumer{}

	c := &connectorImp{
		metricsConsumer: sink,
		cfg:             cfg,
	}

	// costruisci una trace con 1 span la cui durata è latencyToTest ms
	// latenza di test in millisecondi
	var latencyToTest float64 = 120.0
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	start := time.Now().Add(time.Duration(-latencyToTest) * time.Millisecond)
	end := time.Now()
	span.SetStartTimestamp(pcommon.Timestamp(start.UnixNano()))
	span.SetEndTimestamp(pcommon.Timestamp(end.UnixNano()))

	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces failed: %v", err)
	}

	if len(sink.metrics) == 0 {
		t.Fatal("no metrics emitted")
	}

	md := sink.metrics[0]
	metric := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)

	if metric.Name() != cfg.E2ELatencyMetricName {
		t.Fatalf("unexpected metric name: %s", metric.Name())
	}

	h := metric.Histogram()
	if h.DataPoints().Len() != 1 {
		t.Fatalf("expected 1 histogram datapoint, got %d", h.DataPoints().Len())
	}

	dp := h.DataPoints().At(0)

	// Count deve essere 1 (un evento)
	if dp.Count() != 1 {
		t.Fatalf("expected count=1, got %d", dp.Count())
	}

	// Sum deve essere circa latencyToTest(ms)
	sum := dp.Sum()
	epsilon := 0.001 // tolleranza in ms (per conversioni di tempo)
	if !(latencyToTest-epsilon <= sum && sum <= latencyToTest+epsilon) {
		t.Fatalf("unexpected sum: got %f, want ~%f", sum, latencyToTest)
	}

	// Prende i bounds dei buckets e i counts
	bounds := dp.ExplicitBounds().AsRaw()
	counts := dp.BucketCounts().AsRaw()

	// Trova l'indice del bucket dove latencyToTest dovrebbe finire
	expectedIndex := -1
	for i, b := range bounds {
		if latencyToTest <= b {
			expectedIndex = i
			break
		}
	}
	if expectedIndex == -1 {
		// +Inf bucket (last)
		expectedIndex = len(counts) - 1
	}

	// Verifica che solo quel bucket abbia 1 e gli altri 0
	for i, v := range counts {
		if i == expectedIndex {
			if v != 1 {
				t.Fatalf("expected counts[%d]==1, got %d", i, v)
			}
		} else {
			if v != 0 {
				t.Fatalf("expected counts[%d]==0, got %d", i, v)
			}
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
