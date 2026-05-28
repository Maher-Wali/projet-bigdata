# Architecture Suggestions

## 1. Split the dashboard: Grafana + Streamlit

The current Streamlit dashboard covers two distinct concerns in one app. A cleaner split:

**Grafana** for operational monitoring:
- Live streaming metrics (trips/revenue by borough ticking up in real time)
- Kafka consumer lag, Spark executor health, HDFS block status
- Alerting when borough activity deviates > 30% from batch baseline
- Native ClickHouse plugin, zero custom code, built-in auto-refresh at any interval

**Streamlit** (or a Jupyter-based tool like Voilà) for analytical and ML results:
- ML Insights tab: K-Means cluster visualization, fare prediction (R², RMSE)
- Anomaly exploration
- Batch reprocessing trigger button (Grafana is read-only by design)
- Any tab requiring Python logic between the query and the visualization

In production these are often deployed side by side: Grafana as the ops/on-call view,
Streamlit as the data scientist/analyst view.

---

## 2. Move the batch reprocessing trigger into the Streaming vs Batch tab

Currently the reprocessing button lives in a separate "Contrôle Batch" tab, which means
the user has to navigate away from the comparison view to trigger it, then come back to
see the result. It breaks the flow.

The button belongs directly in the "Streaming vs Batch" tab, above or below the right
(batch) column — where the user is already looking at the stale results and naturally
wants to refresh them. The "Contrôle Batch" tab could be kept for the cycle history and
logs, but the trigger itself should be co-located with the data it affects.

---

## 3. Add observability

The pipeline currently has monitoring (pre-defined metrics on the dashboard) but no
observability — if a Spark job silently drops messages or ClickHouse starts returning
stale data, there is no way to diagnose it after the fact.

The three pillars to add:

**Metrics** — already partially covered by the dashboard, but missing infrastructure:
- Kafka consumer lag per topic/partition
- Spark executor memory and GC pressure
- HDFS DataNode block health
- ClickHouse query latency
- Tool: Prometheus + exporters (Kafka Exporter, JMX Exporter for Spark)

**Logs** — currently unstructured or absent:
- Structured logs from each notebook/job: batch size, processing duration, row counts in/out, HDFS write confirmation
- Kafka producer/consumer offsets logged per batch
- Tool: ELK stack (Elasticsearch + Logstash + Kibana) or Loki + Grafana

**Traces** — currently absent:
- End-to-end trace linking a Kafka message through Spark processing to ClickHouse insert
- Useful for diagnosing latency: where does a December trip spend most of its time?
- Tool: OpenTelemetry SDK + Jaeger or Tempo

A minimal first step would be structured logging from the Spark jobs and a Kafka lag
exporter feeding into Grafana — this alone would make pipeline failures diagnosable
rather than opaque.
