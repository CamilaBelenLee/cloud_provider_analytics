-- Cloud Provider Analytics — CQL schema (keyspace created in AstraDB UI)
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
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Q1: daily cost & requests for an org+service over a date range (single-partition read)
-- SELECT usage_date, daily_cost_usd, requests
-- FROM cloud_analytics.org_daily_usage_by_service
-- WHERE org_id=? AND service=? AND usage_date>=? AND usage_date<=?;

-- Q2: top-N services by cost, last 14 days, for an org.
-- Top-N by a computed value spans partitions, so it is aggregated app-side
-- over the org's per-service rows in the window (see notebook).
-- Production alternative — a dedicated mart:
-- CREATE TABLE cloud_analytics.top_services_by_org (
--   org_id text, total_cost double, service text,
--   PRIMARY KEY ((org_id), total_cost, service)
-- ) WITH CLUSTERING ORDER BY (total_cost DESC);
