# Arquitectura — Cloud Provider Analytics (Lambda + Medallion)

```
                         LANDING (inmutable)
                ┌──────────────────────────────────────┐
                │  customers_orgs.csv   users.csv      │
                │  billing_monthly.csv  resources.csv   │
                │  support_tickets.csv  nps_surveys.csv │
                │  marketing_touches.csv                │
                │  usage_events_stream/*.jsonl           │
                └──────────┬────────────┬──────────────┘
                           │            │
              ┌────────────┘            └────────────┐
              ▼ BATCH (states)           ▼ STREAMING (events)
     ┌─────────────────┐        ┌──────────────────────────┐
     │  spark.read.csv │        │ spark.readStream.json     │
     │  explicit schema│        │ explicit nullable schema  │
     │  dedupe by key  │        │ withWatermark("10 min")   │
     │  ingest_ts      │        │ dropDuplicates(event_id)  │
     │  source_file    │        │ checkpoint enabled        │
     └────────┬────────┘        └────────────┬─────────────┘
              │                              │
              ▼                              ▼
     ┌─────────────────────── BRONZE ──────────────────────┐
     │  Parquet tipado, deduplicado, con procedencia       │
     │  orgs/ (by plan_tier)  billing/ (by month)          │
     │  users/ (by role)      events/ (streaming append)   │
     └────────┬──────────────────────────┬─────────────────┘
              │                          │
              ▼                          ▼
     ┌─────────────────────── SILVER ──────────────────────┐
     │  3 reglas de calidad:                               │
     │    R1: event_id NOT NULL                            │
     │    R2: cost_usd_increment >= -0.01                  │
     │    R3: unit NOT NULL cuando value existe             │
     │  Quarantine (filas que fallan) → quarantine/events  │
     │  dropDuplicates global (red de seguridad)           │
     │  Enriquecimiento: broadcast join con orgs_dim       │
     │  Anomalias: z-score + MAD + p99, flag si >=2/3     │
     │  Particionado por usage_date + service              │
     └────────────────────────┬────────────────────────────┘
                              │
                              ▼
     ┌─────────────────────── GOLD ────────────────────────┐
     │  org_daily_usage_by_service                         │
     │    groupBy(org_id, service, usage_date)             │
     │    daily_cost_usd, requests, cpu_hours,             │
     │    storage_gb_hours, genai_tokens, carbon_kg        │
     │  Particionado por usage_date                        │
     └────────────────────────┬────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼ BATCH LOAD                    ▼ SPEED LAYER
     ┌────────────────┐            ┌──────────────────────┐
     │ toLocalIterator│            │ foreachBatch          │
     │ → UPSERT CQL   │            │ tumbling window 1d   │
     └───────┬────────┘            │ outputMode("update") │
             │                     │ trigger(availableNow) │
             │                     └──────────┬───────────┘
             ▼                                ▼
     ┌──────────────── CASSANDRA (AstraDB) ────────────────┐
     │  Keyspace: cloud_analytics                          │
     │  Table: org_daily_usage_by_service                  │
     │  PK: ((org_id, service), usage_date DESC)           │
     │  UPSERT = idempotente (INSERT == UPDATE by key)     │
     │                                                     │
     │  Q1: cost+requests diarios por org+service, rango   │
     │  Q2: top-N services por costo (14d), app-side agg   │
     └─────────────────────────────────────────────────────┘
```

## Patrón: Lambda
- **Batch**: CSV masters/billing → Bronze → Silver → Gold → Cassandra
- **Speed**: JSONL events → Structured Streaming → tumbling window → foreachBatch → Cassandra
- **Serving**: tablas CQL query-first, UPSERT idempotente
