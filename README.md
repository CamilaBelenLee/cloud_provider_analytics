# Cloud Provider Analytics — Big Data 72.80 (ITBA, 1C 2026)

Lambda-architecture analytics pipeline for a cloud provider: ingest, clean, conform, and publish customer data for **FinOps**, **Support**, and **Product/GenAI**, using **PySpark** + **Structured Streaming** on Colab, **Parquet** as the intermediate store, and **Cassandra (AstraDB)** as the query-first serving layer.

**Team:** Camila Lee (63382) · Lucas Perri (62746)

## Run it

The whole MVP runs from `notebooks/cloud_provider_analytics_mvp.ipynb` in Google Colab, top to bottom. Setup (AstraDB Secure Connect Bundle + token, keyspace `cloud_analytics`) is documented in the notebook's first markdown cell. Edit `SCB_PATH`, `ASTRA_TOKEN`, `KEYSPACE` in the serving cell, then Run all.

```
cloud-provider-analytics/
├── README.md
├── notebooks/cloud_provider_analytics_mvp.ipynb   # the deliverable
├── cql/schema.cql                                  # table DDL
├── datalake/landing/                               # provided CSV + usage_events_stream/*.jsonl
└── docs/decision_log.md                            # Lambda, partitions, keys, thresholds, FX
```

## Architecture

**Lambda**, organized over medallion zones in a Data Lake.

- **Batch layer** — the 7 CSV masters/billing/surveys (states, human/period cadence).
- **Speed layer** — `usage_events_stream/*.jsonl` via Structured Streaming (events; watermark, dedup by `event_id`, checkpoint).
- **Serving layer** — query-first **CQL tables** in AstraDB; one table per business question.

### Data Lake zones
- **Landing** — original files, immutable.
- **Bronze** — typed Parquet, deduped, `ingest_ts` + `source_file`, partitioned.
- **Silver** — cleaned, conformed, enriched (broadcast joins to dimensions), 3 quality rules + quarantine, per-service anomaly flag.
- **Gold** — business marts, aggregated, rounded at the serving boundary.

## The data (real schema notes)

- **Events** carry `timestamp` (not `event_time`), and a `metric` field (`requests` | `cpu_hours` | `storage_gb_hours`) that governs `value`. `value` arrives as number, string `"100.0"`, or null → read as **string**, cast with fallback. `carbon_kg`/`genai_tokens` are **v2-only** (after 2025-07-18) → nullable union schema.
- **Billing** carries `exchange_rate_to_usd` (FX source), `credits` often blank (→ 0), `subtotal` occasionally negative.
- **Masters** join on `org_id`; org region is `hq_region` (events have their own `region`).

## Data quality

Three rules on events: `event_id` not null; `cost_usd_increment ≥ -0.01`; `unit` not null when `value` present. Failing rows → **quarantine** Parquet (inspected, not dropped). Streaming dedup (watermark window) is backed by a final batch `dropDuplicates(["event_id"])` for global uniqueness.

## Anomaly detection

Per-service **z-score**, **MAD**, and **p99**; a row is flagged only when **≥2 of 3** agree (consensus → fewer false positives). Anomalies are **flagged, not removed or clamped** — they're real, high-value cost spikes (the FinOps signal), surfaced rather than hidden.

## Serving — why CQL tables, not Document collections

The consigna requires Cassandra query-first modeling with partition/clustering keys. We model **real CQL tables** (e.g. `org_daily_usage_by_service` with `PRIMARY KEY ((org_id, service), usage_date)`) via the DataStax driver + Secure Connect Bundle, loaded with prepared **UPSERTs** (idempotent). We deliberately do **not** use the Document API (schemaless collections), which would discard the partition-key modeling the project is graded on. See `docs/decision_log.md`.

## MVP scope (Segundo Parcial)

Batch→Bronze (3 masters) · Streaming→Bronze (events) · Silver (rules + quarantine + features + anomalies) · Gold FinOps mart · 1 CQL table · Q1 + Q2 · idempotency proof. The remaining marts (revenue, tickets, GenAI, cost-anomaly) and queries Q3–Q5 are the next step toward the final entrega.

## Assumptions & risks

Dataset fits Colab (~60 days events; if it grows, the same Spark code scales to a cluster unchanged). JSONL arrives in minute-scale micro-batches → near-real-time SLA, not strict sub-second. Colab `/content` is ephemeral → mount Drive to persist Parquet/checkpoints across sessions. Late data bounded by a 10-minute watermark. FX taken from `exchange_rate_to_usd`.

## Limitations

File-based streaming (not Kafka); single keyspace; Q2 top-N is a computed value so it's served by scanning the org's partitions and aggregating app-side (a dedicated `top_services_by_org` mart is the production alternative).
