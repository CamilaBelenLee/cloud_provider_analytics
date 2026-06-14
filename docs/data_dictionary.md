# Diccionario de datos (campos clave)

Cubre los archivos que usa el MVP: el feed de eventos y los 3 maestros del pipeline. Los demás CSV (resources, support_tickets, marketing, nps) entran en la entrega final.

## usage_events_stream/*.jsonl — eventos de uso (streaming)

| Campo | Tipo | Dominio / notas |
|---|---|---|
| event_id | string | único; clave de dedupe |
| timestamp | string | event time (no se llama `event_time`); se castea a `event_ts` |
| org_id | string | FK a customers_orgs.org_id |
| resource_id | string | FK a resources.resource_id |
| service | string | compute · storage · database · networking · analytics · genai |
| region | string | región del evento (distinta de `hq_region` de la org) |
| metric | string | requests · cpu_hours · storage_gb_hours — define qué mide `value` |
| value | string | número, `"100.0"` o nulo → se lee string y se castea a `value_num` |
| unit | string | puede ser nulo; si `value` existe y `unit` no, la fila va a quarantine |
| cost_usd_increment | double | costo incremental; ocasionalmente negativo o con spikes (anomalías) |
| schema_version | int | 1 o 2; v2 (desde 2025-07-18) agrega carbon_kg/genai_tokens |
| carbon_kg | double | sólo v2; nullable en v1 |
| genai_tokens | bigint | sólo eventos con service=genai en v2; nullable en el resto |

## customers_orgs.csv — maestro de organizaciones (batch)

| Campo | Tipo | Dominio / notas |
|---|---|---|
| org_id | string | PK |
| org_name | string | |
| industry | string | usado en el enriquecimiento de Silver |
| hq_region | string | región de la org (≠ `region` del evento); partición de Bronze |
| plan_tier | string | a veces inconsistente con is_enterprise (documentado, no se usa en FinOps) |
| is_enterprise | bool | |
| signup_date | date | |
| nps_score | double | tiene un 101 fuera de rango; no se usa en el mart FinOps |

(otros: sales_rep, lifecycle_stage, marketing_source)

## users.csv — maestro de usuarios (batch)

| Campo | Tipo | Dominio / notas |
|---|---|---|
| user_id | string | PK; clave de dedupe |
| org_id | string | FK a customers_orgs |
| email | string | |
| role | string | incluye `admin` (no documentado en la consigna); `StringType` lo acepta igual |
| active | bool | |
| created_at | date | 232 filas con last_login < created_at (inconsistencia temporal, documentada) |
| last_login | date | nullable (139 nulos); partición de Bronze por `role` |

## billing_monthly.csv — facturación (batch)

| Campo | Tipo | Dominio / notas |
|---|---|---|
| invoice_id | string | PK |
| org_id | string | FK a customers_orgs |
| month | date | 3 meses |
| subtotal | double | 13 negativos (notas de crédito) |
| credits | double | ~57% vacío → `coalesce(credits, 0)` |
| taxes | double | |
| currency | string | USD · ARS · EUR; partición de Bronze |
| exchange_rate_to_usd | double | fuente de FX; en USD trae ruido ~1.0 (entrega final) |

## Mart de salida — org_daily_usage_by_service (Gold → Cassandra)

| Campo | Tipo | Notas |
|---|---|---|
| org_id, service, usage_date | text/text/date | PRIMARY KEY ((org_id, service), usage_date) |
| daily_cost_usd | double | sum(cost_usd_increment) del día |
| requests, cpu_hours, storage_gb_hours | double | sum(value WHERE metric=X) |
| genai_tokens | bigint | sum |
| carbon_kg | double | sum |
| anomaly_methods | set\<text\> | métodos que marcaron anomalía ese día (zscore/mad/p99) |