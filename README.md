## Overview

Implementazione di un custom connector per OpenTelemetry Collector.  
Il connector genera metriche OpenTelemetry per latenza e2e e latenza per servizio, a partire dalle tracce contenenti gli spans.  
Le metriche sono esposte sotto forma di istogramma cumulativo (pensate per Prometheus).

La **latenza e2e** è calcolata come differenza tra l'endTime dell'ultimo span della trace (quello con il timestamp maggiore) e lo startTime del primo span (quello con il timestamp minore).  
La **latenza per servizio** viene calcolata con lo stesso metodo ma filtrando per gli span dei singoli servizi, viene inoltre fatto un merge degli intervalli di inizio e fine dei servizi per tenere conto del fatto che un servizio potrebbe andare in esecuzione, poi terminare ed essere richiamato in seguito.  

Per gestire il fatto che le tracce possono arrivare spezzate (gli spans della stessa trace possono non arrivare tutti insieme), lo stato delle trace viene mantenuto in memoria finchè non passa un certo tempo (traceIdleTimeout) senza che arrivino spans di quella trace. Terminato il tempo la traccia viene considerata completa e ne viene calcolata la latenza.  
Viene mantenuto anche lo stato degli istogrammi cumulativi per le diverse tracce in modo che possa essere aggiornato ed esposto all'exporter in modo continuativo.

I **bounds** usati per i **buckets** dell'istogramma sono: [2ms, 4ms, 6ms, 8ms, 10ms, 50ms, 100ms, 200ms, 400ms, 800ms, 1s, 1.4s, 2s, 5s, 10s, 15s, +Inf]

## Warnings

Di seguito vengono riportate alcune avvertenze relative all'uso del connector:

- **Stateful in-memory — perdita di stato al riavvio**: questo connector mantiene lo stato degli istogrammi e delle traces in memoria. Se il Collector o il Pod si riavvia lo stato viene perso e le metriche ricominciano da zero.
Mitigazione: usare un Collector centralizzato o implementare persistence/replication esterna;
- **Dipendenza dalla consegna di tutti gli spans della stessa trace**: la correttezza della latenza dipende dal fatto che gli spans di una stessa trace arrivino allo stesso Collector. Se gli spans sono distribuiti su più Collector la trace può rimanere “spezzata”.
Mitigazione: usare un Collector centralizzato;
- **Clock skew/timestamp non affidabili**: il calcolo della latenza usa gli StartTimestamp/EndTimestamp degli spans. Orologi non sincronizzati tra pod/nodi possono causare maxEnd <= minStart e scarti.
Mitigazione: assicurare NTP/sincronizzazione dei nodi;
- **Pull model Prometheus — emettere snapshot periodici**: Prometheus legge l’endpoint in pull; il connector deve emettere snapshot periodici (anche se nulla è cambiato) o le serie scompaiono tra uno scrape e l’altro;
- **Compatibilità versione Collector/SDK**: il connector è compilato contro una versione specifica del Collector/contrib; aggiornamenti dell’OTel Collector possono rompere API o comportamenti;

## Configurations

Le seguenti impostazioni possono essere configurate (opzionale):

- `e2e_latency_metric_name` (default: `workflow_e2e_latency`): indica con che nome viene esposta la metrica per la latenza e2e;
- `service_latency_metric_name` (default: `workflow_service_latency`): indica con che nome viene esposta la metrica per la latenza dei singoli servizi;
- `service_latency_mode` (default: `false`): indica se si vuole che venga calcolata anche la latenza per i singoli servizi o solo quella e2e;
- `service_name_attribute` (default: `service.name`): indica il nome dell'attributo (span o resource) usato per identificare i singoli servizi nel calcolo della loro latenza;
- `using_istio` (default: `false`): usato per specificare se si sta usando anche Istio per generare gli spans delle traces o solo OTel;
- `trace_idle_timeout` (default: `15s`): indica il tempo di inattività (senza che arrivino spans) dopo il quale una trace viene considerata completa e finalizzata (calcolata la latenza ed esposta come metrica);
- `trace_flush_interval` (default: `3s`): è la frequenza con cui il connector controlla le traces e finalizza quelle scadute (per il quale è passato il trace_idle_timeout);


## Examples

The following is a simple example usage of the workflowe2e connector (preso dal config.yaml di un OTel Collector):  

```
  config:
    receivers:
      otlp:    
        protocols:
          grpc:
            endpoint: "0.0.0.0:4317"
          http:
            endpoint: "0.0.0.0:4318"
    processors:  
      batch:  
        timeout: 200ms
      groupbytrace:
        wait_duration: 5s
        num_traces: 1000 
        num_workers: 1 
    exporters:
      otlp/jaeger:
        endpoint: jaeger-collector.istio-system.svc.cluster.local:4317
        tls:            
          insecure: true
        sending_queue:   
          enabled: true
        retry_on_failure:   
          enabled: true
      prometheus:
        endpoint: "0.0.0.0:8889"

    connectors:
      workflowe2e:
        e2e_latency_metric_name: workflow_e2e_latency
        service_latency_metric_name: workflow_service_latency
        service_latency_mode: true
        service_name_attribute: service.name
        using_istio: true
        trace_idle_timeout: 15s
        trace_flush_interval: 3s
    service:
      telemetry:
        logs:
          level: debug

      pipelines:
        traces: 
          receivers: [otlp] 
          processors: [groupbytrace,batch]  
          exporters: [otlp/jaeger, workflowe2e] 
        metrics/workflowe2e:
          receivers: [workflowe2e]
          exporters: [prometheus]
```

