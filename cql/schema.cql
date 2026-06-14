-- Cloud Provider Analytics — esquema CQL (el keyspace se crea desde la UI de AstraDB)
-- Keyspace: cloud_analytics

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
  anomaly_methods set<text>,        -- tipo colección de CQL: métodos que marcaron anomalía
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Tabla de la Speed Layer (streaming), separada para no pisar el mart del batch:
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_stream (
  org_id text,
  service text,
  usage_date date,
  daily_cost_usd double,
  requests double,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Índice para Q2: qué servicios tiene una org, en una sola partición (evita escanear la tabla grande):
CREATE TABLE IF NOT EXISTS cloud_analytics.services_by_org (
  org_id text,
  service text,
  PRIMARY KEY ((org_id), service)
);

-- Q1: costos y requests diarios por org+servicio en un rango (lectura de una sola partición)
-- SELECT usage_date, daily_cost_usd, requests
-- FROM cloud_analytics.org_daily_usage_by_service
-- WHERE org_id=? AND service=? AND usage_date>=? AND usage_date<=?;

-- Q2: top-N servicios por costo en los últimos 14 días para una org.
-- Los servicios de la org salen de services_by_org (1 partición, sin scan); el costo de cada uno
-- de org_daily_usage_by_service en la ventana, y el top-N se ordena del lado de la app.
-- Alternativa de producción — un mart dedicado que sirve el ranking ya calculado:
-- CREATE TABLE cloud_analytics.top_services_by_org (
--   org_id text, total_cost double, service text,
--   PRIMARY KEY ((org_id), total_cost, service)
-- ) WITH CLUSTERING ORDER BY (total_cost DESC);
-- Se pre-calcula y mantiene al cargar el Gold; el top-N sale con SELECT ... WHERE org_id=? LIMIT N.
