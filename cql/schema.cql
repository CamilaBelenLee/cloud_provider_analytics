-- Cloud Provider Analytics — esquema CQL (el keyspace se crea desde la UI de AstraDB)
-- Keyspace: cloud_analytics

-- =============================================================================
-- Q1: costos y requests diarios por org+servicio, rango de fechas
-- Q2: top-N servicios por costo acumulado (últimos 14d) para una org
--      -> se resuelve filtrando esta misma tabla por org + rango y agregando app-side
-- =============================================================================
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
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Q1: costo y requests diarios por org+servicio en un rango
-- SELECT usage_date, daily_cost_usd, requests
-- FROM cloud_analytics.org_daily_usage_by_service
-- WHERE org_id=? AND service=? AND usage_date>=? AND usage_date<=?;

-- Q2: top-N servicios por costo en los últimos 14 días para una org.
-- El top-N sobre un valor calculado cruza particiones, así que se agrega del lado de la app
-- sobre las filas por servicio de la org en la ventana (ver notebook).

-- ============================================================
-- EVOLUCIÓN (no usada en el MVP) — índice para listar servicios por org.
-- Resuelve Q2 sin escanear la tabla de costos ni hardcodear el catálogo:
--   SELECT service FROM services_by_org WHERE org_id = ?;   (lectura de una sola partición)
-- Se puebla por UPSERT al cargar el Gold; los servicios nuevos entran solos.
-- CREATE TABLE IF NOT EXISTS cloud_analytics.services_by_org (
--   org_id text,
--   service text,
--   PRIMARY KEY ((org_id), service)
-- );
-- ============================================================