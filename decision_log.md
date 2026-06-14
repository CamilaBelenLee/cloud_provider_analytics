# Decision Log — Cloud Provider Analytics

Each entry: the decision, the alternatives, and why. This is the script for defending the pipeline in the video and the "operational depth" the first-delivery feedback asked for.

## 1. Lambda vs Kappa → **Lambda**
The CSV masters/billing/surveys are **states** (human/period cadence); `usage_events_stream` is **events** (fragmented to mimic a live feed). Lambda processes each by its nature. Kappa would require inventing change-capture over static CSVs plus a durable replayable broker (Kafka), which isn't in the provided stack and exceeds scope.

## 2. Serving: real **CQL tables**, not Document-API collections
The consigna requires Cassandra query-first modeling with partition/clustering keys. CQL tables give us `PRIMARY KEY ((org_id, service), usage_date)` and a defensible data-model story. Document collections (astrapy) are easier to load but are schemaless and have **no partition-key modeling** — they would silently drop a graded concept. We accept slightly more load friction for correctness against the brief.

## 3. Partitioning → Parquet partitioned by `usage_date` (and `service` in Silver)
Queries filter by date and service, so partitioning on them enables partition pruning (read only relevant folders). On Colab `spark.sql.shuffle.partitions=8` (small data; 200 default would create tiny tasks).

## 4. Cassandra keys → `PRIMARY KEY ((org_id, service), usage_date)`
Q1 ("daily cost for an org+service over a range") is a single-partition read sorted by date: partition key `(org_id, service)` (always in the WHERE clause), clustering key `usage_date DESC` for range scans. One table per query (denormalized, query-first).

## 5. Features from `metric`, not row counts
`requests`/`cpu_hours`/`storage_gb_hours` are derived as `sum(value WHERE metric=X)`. A `count(*)` of event rows would mislabel "number of events" as "requests" (a real bug we avoided).

## 6. `value` read as **string**, cast with fallback
`value` is type-inconsistent (number, `"100.0"`, null). Declaring it `double` makes Spark null the string rows during parsing. We read as string (loses nothing), keep the raw value for quarantine evidence, and `cast("double")` into `value_num` under our control.

## 7. Anomaly thresholds → per-service, **≥2 of 3** methods
z-score (`>3σ`), MAD (`>3` on the `1.4826·MAD` scale), p99 (`> p99·1.5`). Stats computed **per service** (cost scales differ). Flagged only when ≥2 agree: each method has blind spots (z-score is distorted by the very outliers it seeks; p99 is blunt), so consensus cuts false positives. `K=1.5` is the one freely-chosen knob — tunable. We **flag, never clamp**: anomalies are the FinOps signal, not data to remove.

## 8. Watermark → 10 minutes
Bounds streaming state and late-data tolerance. Matches the assumption that JSONL arrives in minute-scale micro-batches. Larger = more late events accepted but more state held.

## 9. Trigger → `availableNow`
Source is a fixed folder of JSONL files. `availableNow` runs the full streaming engine (watermark, stateful dedup, checkpoint) over all files then **stops** — reproducible, lets the rest of the notebook run, avoids the infinite-query-in-a-notebook antipattern. Production against a live feed would switch to `processingTime`; no other code changes.

## 10. Idempotency → checkpoint + natural keys + UPSERT
Checkpoint skips already-consumed files; `event_id` and `(org_id, service, usage_date)` are natural keys; Cassandra INSERT==UPSERT overwrites by key. Re-running leaves row counts unchanged (proven in the notebook).

## 11. FX normalization → multiply by `exchange_rate_to_usd`
Revenue normalized via the per-invoice `exchange_rate_to_usd`. `credits` coalesced to 0. USD rows carry noisy ~1.0 rates; we use the field as-is and note the choice (alternative: force USD→1.0). (Revenue mart is part of the final entrega, not the MVP.)

## 12. Dedup → streaming watermark dedup + final batch dedup
Streaming `dropDuplicates(["event_id"])` only dedupes within the watermark window. A final batch `dropDuplicates` in Silver guarantees global uniqueness for the marts. Deliberate belt-and-suspenders, not redundancy.
