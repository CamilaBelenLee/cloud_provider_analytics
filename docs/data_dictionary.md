# Diccionario de datos (campos clave)

Cubre los archivos que consume el pipeline: el feed de eventos y los 4 maestros. `resources.csv`, `marketing_touches.csv` y `nps_surveys.csv` quedan sin usar — ninguna de las 5 consultas los necesita.

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
| nps_score | double | un solo valor fuera de rango (101, `org_pac56t4u`). Los 16 negativos son válidos: NPS va de −100 a +100. No entra en ningún mart |

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
| exchange_rate_to_usd | double | USD 0,855–1,118 (media 0,9977) → **se fuerza a 1.0**; EUR 0,998–1,198 y ARS 0,00133–0,00162 se respetan |

## support_tickets.csv — tickets de soporte (batch)

| Campo | Tipo | Dominio / notas |
|---|---|---|
| ticket_id | string | PK; clave de dedupe |
| org_id | string | FK a customers_orgs |
| category | string | integration · billing · availability · usability · performance · security; partición de Bronze |
| severity | string | low (412) · medium (328) · high (204) · critical (56); partición de Silver |
| created_at | date | 2025-05-09 a 2025-08-31, 115 fechas distintas (arranca antes que los eventos) |
| resolved_at | date | nulo = ticket abierto (240 de 1000) |
| csat | double | 254 nulos → se dejan NULL (`avg` los saltea). Distribución 0–7 que ajusta a una normal redondeada: **no hay valores fuera de rango**, ver decision_log #26 |
| sla_breached | bool | 95 true de 1000 |

## Marts de salida (Gold → Cassandra)

### org_daily_usage_by_service — Q1, Q2 · 11.050 filas

| Campo | Tipo | Notas |
|---|---|---|
| org_id, service, usage_date | text/text/date | PRIMARY KEY ((org_id, service), usage_date) |
| daily_cost_usd | double | sum(cost_usd_increment) del día |
| requests, cpu_hours, storage_gb_hours | double | sum(value WHERE metric=X) |
| genai_tokens | bigint | sum |
| carbon_kg | double | sum |
| anomaly_methods | set\<text\> | métodos que marcaron anomalía ese día (zscore/mad/p99) |
### tickets_by_org_date — Q3 · 944 filas

| Campo | Tipo | Notas |
|---|---|---|
| org_id, ticket_date | text/date | PRIMARY KEY ((org_id), ticket_date) |
| total_tickets, critical_tickets, tickets_abiertos | bigint | conteos del día |
| avg_csat | double | NULL si no hubo respuestas ese día (229 org-días) |
| sla_breach_rate | double | 0..1 — `avg(sla_breached::int)` |
| severities | map\<text,int\> | conteo por severidad, p. ej. `{'low':3,'critical':1}` |

### revenue_by_org_month — Q4 · 240 filas

| Campo | Tipo | Notas |
|---|---|---|
| org_id, month | text/date | PRIMARY KEY ((org_id), month) |
| subtotal_usd, credits_usd, taxes_usd | double | componentes ya normalizados a USD |
| net_revenue_usd | double | `(subtotal − credits + taxes) × fx` |
| currencies | set\<text\> | monedas en las que facturó la org ese mes |
| notas_credito | int | invoices con subtotal < 0 |

### genai_tokens_by_org_date — Q5 · 1.131 filas

| Campo | Tipo | Notas |
|---|---|---|
| org_id, usage_date | text/date | PRIMARY KEY ((org_id), usage_date) |
| total_tokens | bigint | 0 antes del 2025-07-18 (v1 no trae el campo) |
| estimated_cost_usd | double | `sum(cost_usd_increment)` de los eventos genai |

### cost_anomaly_mart — requisito 4 · 89 filas

| Campo | Tipo | Notas |
|---|---|---|
| org_id, usage_date, service | text/date/text | PRIMARY KEY ((org_id), usage_date, service) |
| eventos_anomalos | int | eventos marcados ese día |
| costo_anomalo_usd, costo_max_usd | double | suma y máximo del costo anómalo |
| anomaly_methods | set\<text\> | métodos que coincidieron (≥2 de 3) |
