## Overview

Implementation of a custom connector for the OpenTelemetry Collector.
The connector generates OpenTelemetry metrics for end-to-end (e2e) latency and per-service latency from traces that contain spans.
Metrics are exposed as cumulative histograms (suitable for Prometheus).

**End-to-end latency** is calculated as the difference between the endTime of the last span in the trace (the one with the largest timestamp) and the startTime of the first span (the one with the smallest timestamp).  
**Per-service latency** is calculated using the same method but filtering spans by individual services; the start/end intervals of a service are also merged to account for the fact that a service may start, stop, and be invoked again later.

To handle traces that may arrive fragmented (spans belonging to the same trace may not arrive all at once), trace state is kept in memory until a configured time (traceIdleTimeout) passes without receiving any spans for that trace. When that timeout elapses the trace is considered complete and its latency is computed. Otherwise, its possible to specify the number of spans to be expected for a trace (there is the timeout still).
The state of the cumulative histograms for different traces is also maintained so they can be continuously updated and exposed to the exporter.
Its also possible to connect to a database for saving data for future analysis.

The **bounds** used for the **histogram buckets** are: [2ms, 4ms, 6ms, 8ms, 10ms, 50ms, 100ms, 200ms, 400ms, 800ms, 1s, 1.4s, 2s, 5s, 10s, 15s, +Inf]

## Warnings

The following warnings relate to the use of the connector:  

- **Stateful in-memory — state loss on restart**: this connector keeps histogram and trace state in memory. If the Collector or Pod restarts, the state is lost and metrics start again from zero.  
Mitigation: use a centralized Collector or implement external persistence/replication;
- **Dependency on delivery of all spans for the same trace**: correct latency calculation depends on spans belonging to the same trace arriving at the same Collector. If spans are distributed across multiple Collectors the trace may remain “fragmented.”  
Mitigation: use a centralized Collector;
- **Clock skew/unreliable timestamps**: latency calculation uses spans’ StartTimestamp/EndTimestamp. Unsynchronized clocks between pods/nodes can lead to maxEnd <= minStart and incorrect results.  
Mitigation: ensure NTP / node time synchronization;
- **Pull model Prometheus — emit periodic snapshots**: Prometheus scrapes endpoints in a pull model; the connector must emit periodic snapshots (even if nothing changed) or series may disappear between scrapes;
- **Collector/SDK version compatibility**: the connector is built against a specific Collector/contrib version; updates to the OTel Collector may break APIs or behavior.

## Configurations

The following settings can be configured (optional):  

- `e2e_latency_metric_name` (default: `workflow_e2e_latency`): name used to expose the e2e latency metric;
- `service_latency_metric_name` (default: `workflow_service_latency`): name used to expose the per-service latency metric;
- `service_latency_mode` (default: `true`): whether to compute per-service latency as well as e2e latency, or only e2e;
- `service_name_attribute` (default: `service.name`): the attribute name (span or resource) used to identify individual services when computing their latency;
- `experiment_name_attribute` (default: `experiment.name`): resource attribute from which to read the experiment name;
- `experiment_name_header` (default: `experiment_name`): “baggage-like” key used to extract the value from the attribute;
- `using_istio` (default: `true`): whether Istio is also used to generate spans for traces, or only OpenTelemetry if false. The logic used to parse service and operation names changes if we are using Istio or only OTel;
- `n_spans_for_trace` (default: `0`): number of spans to be expected for a trace (if 0 only the timeout mechanism is used);
- `trace_idle_timeout` (default: `15s`): idle time (no spans received) after which a trace is considered complete and finalized (latency computed and exposed as a metric);
- `trace_flush_interval` (default: `3s`): how often the connector checks traces and finalizes those that have expired (i.e. passed the trace_idle_timeout);
- `db_url` (default: ""): URL of a database (PostgreSQL) to save data for future analysis (if used);


## Examples

The following is a simple example usage of the workflowe2e connector (taken from an OTel Collector config.yaml):  

```
  connectors:
      workflowe2e:
        e2e_latency_metric_name: workflow_e2e_latency
        service_latency_metric_name: workflow_service_latency
        service_latency_mode: true
        service_name_attribute: service.name
        experiment_name_attribute: experiment.name
        experiment_name_header: experiment_name
        using_istio: true
        n_spans_for_trace: 10
        trace_idle_timeout: 80s
        trace_flush_interval: 5s
        db_url: "postgres://user:password@postgres-service.observability.svc.cluster.local:5432/tracing_db?sslmode=disable"
```

## Usage

To use this component you need to build it first, for example you can use an existing distribution of an OpenTelemetry Collector and extend it with this connector.
Example of steps:

1) In the YAML manifest of the OTel Collector distribution add:

```
dist:
  module: github.com/gabrieleDalo/otelcol-workflowe2e
  name: otelcol-workflowe2e
  description: OTel Collector Contrib + workflow E2E connector
  version: 0.1.0
  output_path: ./_build
  build_tags: "grpcnotrace"

...

connectors:
  ...
  - gomod: github.com/gabrieleDalo/workflowe2e-connector v0.1.36
    import: github.com/gabrieleDalo/workflowe2e-connector/connector/workflowe2e
...
```

2) Use a tool such as OCB (https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder) to build the new OTel Collector distribution.
Based on the architecture, such as for Kubernetes:

```
export GOOS=linux
export GOARCH=amd64
./ocb --config manifest.yaml
```

3) If all goes well, a _build folder has been generated with the exec of our custom collector. Now we have the build, we need to convert it into an image we can to use in the Collector manifest for Kubernetes.
There are different ways to do it, an example using minikube on Kubernetes and Docker is:

- Write a Dockerfile and put it in the same folder as the exec, example:
  
```
FROM gcr.io/distroless/base-debian12
WORKDIR /otel
COPY otelcol-workflowe2e /otel/otelcol
EXPOSE 4317 4318 8889
USER nonroot:nonroot
ENTRYPOINT ["/otel/otelcol"]
```

- After that I can build the image on minikube so that the cluster sees it right away, so I start minikube and then do:
  
```
eval $(minikube docker-env)
docker build --no-cache -t workflowe2e/otelcol:0.1.0 .
docker images | grep workflowe2e/otelcol
```

These commands tell Minikube to use the internal Docker daemon. The image is built directly for Minikube, and you can see if it was added correctly.
This way, Minikube will find the image directly; you don't need to push it to Docker Hub (image: workflowe2e/otelcol:0.1.0).

4) Then just modify the OTel Collector manifest to use the new image and custom connector and restart it, example:

```
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: otel-collector
  namespace: observability
spec:
  # Let's define the image to use for the Collector, in this case it's the contrib which is the extended version that includes many additional receivers/processors/exporters/connectors
  image: workflowe2e/otelcol:0.1.0    # Custom image for the Collector, I used the Collector contrib to which I added my custom connector
...
```
