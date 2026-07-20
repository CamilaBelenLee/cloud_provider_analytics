-- Cloud Provider Analytics — esquema CQL (el keyspace se crea desde la UI de AstraDB)
-- Keyspace: cloud_analytics
-- Una tabla por consulta (query-first). La partition key es lo que va con = en el WHERE;
-- la clustering key ordena y habilita el rango.

-- ---------------------------------------------------------------- Q1 y Q2
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_by_service (
  org_id text,
  service text,
  usage_date date,
  daily_cost_usd double,
  requests double,
  cpu_hours double,
  storage_gb_hours double,
  genai_tokens bigint,
  carbon_kg double,
  anomaly_methods set<text>,        -- colección: métodos que marcaron anomalía ese día
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Índice para Q2: qué servicios tiene una org, en una sola partición (evita escanear la tabla grande):
CREATE TABLE IF NOT EXISTS cloud_analytics.services_by_org (
  org_id text,
  service text,
  PRIMARY KEY ((org_id), service)
);

-- ---------------------------------------------------------------- Q3
-- severities guarda el conteo por severidad, que se pierde al agrupar por (org, día).
-- Son 4 claves posibles (low/medium/high/critical), así que la colección queda acotada.
CREATE TABLE IF NOT EXISTS cloud_analytics.tickets_by_org_date (
  org_id text,
  ticket_date date,
  total_tickets bigint,
  critical_tickets bigint,
  tickets_abiertos bigint,          -- resolved_at NULL = sigue abierto (240 de 1000)
  avg_csat double,                  -- NULL si esa org no tuvo respuestas ese día
  sla_breach_rate double,           -- 0..1
  severities map<text,int>,         -- colección: {'low':3,'critical':1}
  PRIMARY KEY ((org_id), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- ---------------------------------------------------------------- Q4
-- currencies guarda en qué monedas facturó la org ese mes; se pierde al normalizar a USD.
CREATE TABLE IF NOT EXISTS cloud_analytics.revenue_by_org_month (
  org_id text,
  month date,
  subtotal_usd double,
  credits_usd double,
  taxes_usd double,
  net_revenue_usd double,           -- (subtotal - credits + taxes) * fx
  currencies set<text>,             -- colección: {'USD'} / {'ARS'} / ...
  notas_credito int,                -- invoices con subtotal < 0
  PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- ---------------------------------------------------------------- Q5
CREATE TABLE IF NOT EXISTS cloud_analytics.genai_tokens_by_org_date (
  org_id text,
  usage_date date,
  total_tokens bigint,
  estimated_cost_usd double,
  PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- ---------------------------------------------------------------- requisito 4 de la consigna
CREATE TABLE IF NOT EXISTS cloud_analytics.cost_anomaly_mart (
  org_id text,
  usage_date date,
  service text,
  eventos_anomalos int,
  costo_anomalo_usd double,
  costo_max_usd double,
  anomaly_methods set<text>,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- ---------------------------------------------------------------- Speed Layer
-- Tabla de la Speed Layer (streaming), separada para no pisar el mart del batch:
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_stream (
  org_id text,
  service text,
  usage_date date,
  daily_cost_usd double,
  requests double,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);


-- ================================================================
--  Las 5 consultas
-- ================================================================

-- Q1: costos y requests diarios por org y servicio en un rango de fechas.
--     Lectura de una sola partición, con rango sobre la clustering key.
-- SELECT usage_date, daily_cost_usd, requests
-- FROM cloud_analytics.org_daily_usage_by_service
-- WHERE org_id=? AND service=? AND usage_date>=? AND usage_date<=?;

-- Q2: top-N servicios por costo acumulado en los últimos 14 días para una org.
--     Los servicios salen de services_by_org (1 partición, sin scan); el costo de cada uno
--     de org_daily_usage_by_service en la ventana, y el top-N se ordena del lado de la app.
-- SELECT service FROM cloud_analytics.services_by_org WHERE org_id=?;
-- SELECT daily_cost_usd FROM cloud_analytics.org_daily_usage_by_service
-- WHERE org_id=? AND service=? AND usage_date>=? AND usage_date<=?;
--
--     Alternativa de producción — un mart dedicado que sirve el ranking ya calculado:
-- CREATE TABLE cloud_analytics.top_services_by_org (
--   org_id text, total_cost double, service text,
--   PRIMARY KEY ((org_id), total_cost, service)
-- ) WITH CLUSTERING ORDER BY (total_cost DESC);
--     Se pre-calcula al cargar el Gold; el top-N sale con SELECT ... WHERE org_id=? LIMIT N.
--     No lo materializamos: obliga a borrar y reescribir la partición entera de la org en cada
--     carga (el ranking está en la clustering key), y con 6 servicios por org el orden del lado
--     de la app cuesta 6 lecturas de partición. Se justificaría con muchos más servicios.

-- Q3: evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
-- SELECT ticket_date, total_tickets, critical_tickets, tickets_abiertos,
--        sla_breach_rate, avg_csat, severities
-- FROM cloud_analytics.tickets_by_org_date
-- WHERE org_id=? AND ticket_date>=? AND ticket_date<=?;

-- Q4: revenue mensual con créditos e impuestos, normalizado a USD.
-- SELECT month, subtotal_usd, credits_usd, taxes_usd, net_revenue_usd, currencies, notas_credito
-- FROM cloud_analytics.revenue_by_org_month
-- WHERE org_id=?;

-- Q5: tokens GenAI y costo estimado por día.
--     genai_tokens sólo existe desde schema_version=2 (2025-07-18): los días previos dan
--     0 tokens con costo > 0. Es la evolución de esquema, no un hueco de datos.
-- SELECT usage_date, total_tokens, estimated_cost_usd
-- FROM cloud_analytics.genai_tokens_by_org_date
-- WHERE org_id=?;

-- Extra (requisito 4): anomalías de costo por org, día y servicio.
-- SELECT usage_date, service, eventos_anomalos, costo_anomalo_usd, anomaly_methods
-- FROM cloud_analytics.cost_anomaly_mart
-- WHERE org_id=?;
