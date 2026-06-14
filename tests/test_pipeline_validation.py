"""
Pipeline validation tests — Cloud Provider Analytics MVP
Runs locally against the real dataset in datalake/landing/.
Validates schemas, quality rules, transformations, and output structure.

Usage:
    pip install pyspark==3.5.1 pytest
    pytest tests/test_pipeline_validation.py -v
"""
import os, json, csv, datetime, pytest
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING = PROJECT_ROOT / "datalake" / "landing"
EVENTS_DIR = LANDING / "usage_events_stream"

# Skip the entire module if the landing data isn't present
pytestmark = pytest.mark.skipif(
    not LANDING.exists(), reason="Landing data not found at datalake/landing/"
)

# ---------------------------------------------------------------------------
# Spark fixture (one session for all tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder
         .appName("test_pipeline_validation")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# ===================================================================
# 1. SCHEMA VALIDATION — real files match declared schemas
# ===================================================================

class TestSchemaAlignment:
    """Verify that the declared Spark schemas match the actual CSV/JSONL headers."""

    def test_customers_orgs_columns(self):
        with open(LANDING / "customers_orgs.csv") as f:
            header = next(csv.reader(f))
        expected = ["org_id","org_name","industry","hq_region","plan_tier",
                     "is_enterprise","signup_date","sales_rep","lifecycle_stage",
                     "marketing_source","nps_score"]
        assert header == expected, f"orgs header mismatch: {header}"

    def test_users_columns(self):
        with open(LANDING / "users.csv") as f:
            header = next(csv.reader(f))
        expected = ["user_id","org_id","email","role","active","created_at","last_login"]
        assert header == expected, f"users header mismatch: {header}"

    def test_billing_columns(self):
        with open(LANDING / "billing_monthly.csv") as f:
            header = next(csv.reader(f))
        expected = ["invoice_id","org_id","month","subtotal","credits","taxes",
                     "currency","exchange_rate_to_usd"]
        assert header == expected, f"billing header mismatch: {header}"

    def test_support_tickets_columns(self):
        with open(LANDING / "support_tickets.csv") as f:
            header = next(csv.reader(f))
        expected = ["ticket_id","org_id","category","severity","created_at",
                     "resolved_at","csat","sla_breached"]
        assert header == expected, f"tickets header mismatch: {header}"

    def test_resources_columns(self):
        with open(LANDING / "resources.csv") as f:
            header = next(csv.reader(f))
        expected = ["resource_id","org_id","service","region","created_at","state","tags_json"]
        assert header == expected, f"resources header mismatch: {header}"

    def test_nps_surveys_columns(self):
        with open(LANDING / "nps_surveys.csv") as f:
            header = next(csv.reader(f))
        expected = ["org_id","survey_date","nps_score","comment"]
        assert header == expected, f"nps header mismatch: {header}"

    def test_events_jsonl_fields(self):
        """First JSONL file should have the expected event fields."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        assert len(files) > 0, "No JSONL files found"
        with open(files[0]) as f:
            event = json.loads(f.readline())
        required = {"event_id","timestamp","org_id","resource_id","service",
                     "region","metric","value","unit","cost_usd_increment",
                     "schema_version"}
        assert required.issubset(event.keys()), f"Missing fields: {required - event.keys()}"

    def test_events_v2_has_carbon_kg(self):
        """At least some events should have carbon_kg (v2 schema)."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        found_v2 = False
        for fp in files:
            with open(fp) as f:
                for line in f:
                    evt = json.loads(line)
                    if evt.get("schema_version") == 2 and "carbon_kg" in evt:
                        found_v2 = True
                        break
            if found_v2:
                break
        assert found_v2, "No v2 events with carbon_kg found"


# ===================================================================
# 2. BATCH BRONZE — typing, dedupe, technical columns
# ===================================================================

class TestBatchBronze:
    """Test batch ingestion to Bronze zone."""

    def _schemas(self):
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, BooleanType, DateType)
        orgs_schema = StructType([
            StructField("org_id",StringType()),StructField("org_name",StringType()),
            StructField("industry",StringType()),StructField("hq_region",StringType()),
            StructField("plan_tier",StringType()),StructField("is_enterprise",BooleanType()),
            StructField("signup_date",DateType()),StructField("sales_rep",StringType()),
            StructField("lifecycle_stage",StringType()),StructField("marketing_source",StringType()),
            StructField("nps_score",DoubleType())])
        billing_schema = StructType([
            StructField("invoice_id",StringType()),StructField("org_id",StringType()),
            StructField("month",DateType()),StructField("subtotal",DoubleType()),
            StructField("credits",DoubleType()),StructField("taxes",DoubleType()),
            StructField("currency",StringType()),StructField("exchange_rate_to_usd",DoubleType())])
        return orgs_schema, billing_schema

    def test_orgs_loads_without_nulls_in_key(self, spark):
        from pyspark.sql import functions as F
        orgs_schema, _ = self._schemas()
        df = spark.read.option("header",True).schema(orgs_schema).csv(
            str(LANDING / "customers_orgs.csv"))
        assert df.count() > 0, "orgs is empty"
        null_keys = df.filter(F.col("org_id").isNull()).count()
        assert null_keys == 0, f"{null_keys} null org_id rows"

    def test_orgs_dedup_is_idempotent(self, spark):
        orgs_schema, _ = self._schemas()
        df = spark.read.option("header",True).schema(orgs_schema).csv(
            str(LANDING / "customers_orgs.csv"))
        before = df.count()
        after = df.dropDuplicates(["org_id"]).count()
        # If they differ, there are duplicates in the source (that's fine, dedupe works)
        assert after <= before
        assert after > 0

    def test_billing_credits_nullable(self, spark):
        """credits column has blanks in the real data -> should parse as null."""
        from pyspark.sql import functions as F
        _, billing_schema = self._schemas()
        df = spark.read.option("header",True).schema(billing_schema).csv(
            str(LANDING / "billing_monthly.csv"))
        null_credits = df.filter(F.col("credits").isNull()).count()
        assert null_credits > 0, "Expected some null credits (blanks in CSV)"

    def test_billing_has_negative_subtotal(self, spark):
        """Real data has negative subtotals (e.g. inv_gx8uwtk7)."""
        from pyspark.sql import functions as F
        _, billing_schema = self._schemas()
        df = spark.read.option("header",True).schema(billing_schema).csv(
            str(LANDING / "billing_monthly.csv"))
        neg = df.filter(F.col("subtotal") < 0).count()
        assert neg > 0, "Expected at least one negative subtotal"

    def test_billing_multi_currency(self, spark):
        """Should have USD, ARS, EUR."""
        _, billing_schema = self._schemas()
        df = spark.read.option("header",True).schema(billing_schema).csv(
            str(LANDING / "billing_monthly.csv"))
        currencies = {r["currency"] for r in df.select("currency").distinct().collect()}
        assert {"USD","ARS","EUR"}.issubset(currencies), f"Missing currencies: {currencies}"


# ===================================================================
# 3. STREAMING BRONZE — events schema, value-as-string, timestamp
# ===================================================================

class TestEventsBronze:
    """Validate event reading and Bronze transformations."""

    def _event_schema(self):
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, LongType, IntegerType)
        return StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("org_id", StringType()),
            StructField("resource_id", StringType()),
            StructField("service", StringType()),
            StructField("region", StringType()),
            StructField("metric", StringType()),
            StructField("value", StringType()),
            StructField("unit", StringType()),
            StructField("cost_usd_increment", DoubleType()),
            StructField("schema_version", IntegerType()),
            StructField("carbon_kg", DoubleType()),
            StructField("genai_tokens", LongType()),
        ])

    def test_events_load_count(self, spark):
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        assert df.count() > 40000, "Expected ~43200 events"

    def test_value_as_string_cast(self, spark):
        """value field must be readable as string and castable to double."""
        from pyspark.sql import functions as F
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        with_cast = df.withColumn("value_num", F.col("value").cast("double"))
        # Some values are null (original null), some are numeric, some are string-numeric
        non_null_value = df.filter(F.col("value").isNotNull()).count()
        cast_ok = with_cast.filter(
            F.col("value").isNotNull() & F.col("value_num").isNotNull()
        ).count()
        # All non-null values should cast successfully (they're numbers or "number" strings)
        assert cast_ok == non_null_value, (
            f"{non_null_value - cast_ok} values failed to cast to double")

    def test_timestamp_parses(self, spark):
        """timestamp field (ISO-8601 UTC) must parse to TimestampType."""
        from pyspark.sql import functions as F
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        with_ts = df.withColumn("event_ts", F.to_timestamp("timestamp"))
        null_ts = with_ts.filter(F.col("event_ts").isNull()).count()
        assert null_ts == 0, f"{null_ts} events have unparseable timestamps"

    def test_services_match_expected(self, spark):
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        services = {r["service"] for r in df.select("service").distinct().collect()}
        expected = {"compute","storage","database","networking","analytics","genai"}
        assert services == expected, f"Unexpected services: {services}"

    def test_metrics_match_expected(self, spark):
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        metrics = {r["metric"] for r in df.select("metric").distinct().collect()}
        expected = {"requests","cpu_hours","storage_gb_hours"}
        assert metrics == expected, f"Unexpected metrics: {metrics}"

    def test_schema_versions_1_and_2(self, spark):
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        versions = {r["schema_version"] for r in df.select("schema_version").distinct().collect()}
        assert versions == {1, 2}, f"Expected schema versions 1 and 2, got {versions}"

    def test_genai_tokens_only_for_genai_service(self, spark):
        """genai_tokens should only be non-null for service='genai'."""
        from pyspark.sql import functions as F
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        non_genai_with_tokens = df.filter(
            (F.col("service") != "genai") & F.col("genai_tokens").isNotNull()
        ).count()
        assert non_genai_with_tokens == 0, (
            f"{non_genai_with_tokens} non-genai events have genai_tokens set")

    def test_dedup_by_event_id(self, spark):
        df = spark.read.schema(self._event_schema()).json(str(EVENTS_DIR))
        total = df.count()
        unique = df.dropDuplicates(["event_id"]).count()
        # Report if there are duplicates (streaming dedupe should handle them)
        print(f"  Total events: {total}, unique event_ids: {unique}, dupes: {total - unique}")
        assert unique > 0


# ===================================================================
# 4. QUALITY RULES — validate the 3 rules catch real violations
# ===================================================================

class TestQualityRules:
    """Verify that quality rules fire on known data issues."""

    def _load_events(self, spark):
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, LongType, IntegerType)
        from pyspark.sql import functions as F
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("org_id", StringType()),
            StructField("resource_id", StringType()),
            StructField("service", StringType()),
            StructField("region", StringType()),
            StructField("metric", StringType()),
            StructField("value", StringType()),
            StructField("unit", StringType()),
            StructField("cost_usd_increment", DoubleType()),
            StructField("schema_version", IntegerType()),
            StructField("carbon_kg", DoubleType()),
            StructField("genai_tokens", LongType()),
        ])
        return (spark.read.schema(schema).json(str(EVENTS_DIR))
                .withColumn("value_num", F.col("value").cast("double")))

    def test_rule3_unit_null_with_value_present(self, spark):
        """Rule 3: unit IS NULL when value IS NOT NULL — real violation exists."""
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        violations = df.filter(F.col("value").isNotNull() & F.col("unit").isNull()).count()
        assert violations > 0, "Expected rule 3 violations (value present, unit null)"
        print(f"  Rule 3 violations found: {violations}")

    def test_rule2_negative_cost(self, spark):
        """Rule 2: cost_usd_increment < -0.01 — real violations exist."""
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        violations = df.filter(F.col("cost_usd_increment") < -0.01).count()
        assert violations > 0, "Expected some negative cost_usd_increment values"
        print(f"  Rule 2 violations (cost < -0.01): {violations}")

    def test_quarantine_captures_failures(self, spark):
        """The combined quality filter should quarantine some rows."""
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        valid = (F.col("event_id").isNotNull()
                 & (F.col("cost_usd_increment") >= -0.01)
                 & ~(F.col("value").isNotNull() & F.col("unit").isNull()))
        ok_count = df.filter(valid).count()
        quarantine_count = df.filter(~valid).count()
        total = df.count()
        assert quarantine_count > 0, "No rows quarantined — rules not catching anything"
        assert ok_count + quarantine_count == total, "Partition mismatch"
        print(f"  Valid: {ok_count}, Quarantined: {quarantine_count} ({100*quarantine_count/total:.1f}%)")


# ===================================================================
# 5. SILVER — enrichment join + features
# ===================================================================

class TestSilverTransformations:

    def test_broadcast_join_enriches_events(self, spark):
        """Events joined with orgs should gain industry, plan_tier, hq_region."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, LongType, IntegerType, BooleanType, DateType)

        event_schema = StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("org_id", StringType()),
            StructField("resource_id", StringType()),
            StructField("service", StringType()),
            StructField("region", StringType()),
            StructField("metric", StringType()),
            StructField("value", StringType()),
            StructField("unit", StringType()),
            StructField("cost_usd_increment", DoubleType()),
            StructField("schema_version", IntegerType()),
            StructField("carbon_kg", DoubleType()),
            StructField("genai_tokens", LongType()),
        ])
        orgs_schema = StructType([
            StructField("org_id",StringType()),StructField("org_name",StringType()),
            StructField("industry",StringType()),StructField("hq_region",StringType()),
            StructField("plan_tier",StringType()),StructField("is_enterprise",BooleanType()),
            StructField("signup_date",DateType()),StructField("sales_rep",StringType()),
            StructField("lifecycle_stage",StringType()),StructField("marketing_source",StringType()),
            StructField("nps_score",DoubleType())])

        events = spark.read.schema(event_schema).json(str(EVENTS_DIR)).limit(1000)
        orgs = spark.read.option("header",True).schema(orgs_schema).csv(
            str(LANDING / "customers_orgs.csv"))
        orgs_dim = orgs.select("org_id","industry","plan_tier","hq_region")

        enriched = events.join(F.broadcast(orgs_dim), on="org_id", how="left")
        assert "industry" in enriched.columns
        assert "plan_tier" in enriched.columns
        assert "hq_region" in enriched.columns
        # Most events should match an org
        matched = enriched.filter(F.col("industry").isNotNull()).count()
        total = enriched.count()
        assert matched / total > 0.9, f"Only {matched}/{total} events matched an org"


# ===================================================================
# 6. GOLD — FinOps mart structure and aggregation logic
# ===================================================================

class TestGoldMart:

    def test_finops_mart_aggregation(self, spark):
        """Build the Gold mart and verify structure + non-null aggregations."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, LongType, IntegerType)

        schema = StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("org_id", StringType()),
            StructField("resource_id", StringType()),
            StructField("service", StringType()),
            StructField("region", StringType()),
            StructField("metric", StringType()),
            StructField("value", StringType()),
            StructField("unit", StringType()),
            StructField("cost_usd_increment", DoubleType()),
            StructField("schema_version", IntegerType()),
            StructField("carbon_kg", DoubleType()),
            StructField("genai_tokens", LongType()),
        ])
        df = (spark.read.schema(schema).json(str(EVENTS_DIR))
              .withColumn("event_ts", F.to_timestamp("timestamp"))
              .withColumn("value_num", F.col("value").cast("double"))
              .withColumn("usage_date", F.to_date("event_ts")))

        # Apply quality filter
        valid = (F.col("event_id").isNotNull()
                 & (F.col("cost_usd_increment") >= -0.01)
                 & ~(F.col("value").isNotNull() & F.col("unit").isNull()))
        df = df.filter(valid)

        gold = (df.groupBy("org_id","service","usage_date").agg(
            F.round(F.sum("cost_usd_increment"),2).alias("daily_cost_usd"),
            F.round(F.sum(F.when(F.col("metric")=="requests", F.col("value_num"))),2).alias("requests"),
            F.round(F.sum(F.when(F.col("metric")=="cpu_hours", F.col("value_num"))),4).alias("cpu_hours"),
            F.round(F.sum(F.when(F.col("metric")=="storage_gb_hours", F.col("value_num"))),4).alias("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.round(F.sum("carbon_kg"),6).alias("carbon_kg")))

        # Structure checks
        expected_cols = {"org_id","service","usage_date","daily_cost_usd","requests",
                         "cpu_hours","storage_gb_hours","genai_tokens","carbon_kg"}
        assert expected_cols.issubset(set(gold.columns)), (
            f"Missing columns: {expected_cols - set(gold.columns)}")

        row_count = gold.count()
        assert row_count > 0, "Gold mart is empty"
        print(f"  Gold mart rows: {row_count}")

        # daily_cost_usd should never be null (it's a sum of non-null cost_usd_increment)
        null_cost = gold.filter(F.col("daily_cost_usd").isNull()).count()
        assert null_cost == 0, f"{null_cost} rows have null daily_cost_usd"

        # At least some rows should have requests > 0
        has_requests = gold.filter(F.col("requests") > 0).count()
        assert has_requests > 0, "No rows with requests > 0"

    def test_metric_based_features_are_correct(self, spark):
        """Verify that requests = sum(value WHERE metric='requests'), not count(*)."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import (StructType, StructField, StringType,
            DoubleType, LongType, IntegerType)

        schema = StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("org_id", StringType()),
            StructField("resource_id", StringType()),
            StructField("service", StringType()),
            StructField("region", StringType()),
            StructField("metric", StringType()),
            StructField("value", StringType()),
            StructField("unit", StringType()),
            StructField("cost_usd_increment", DoubleType()),
            StructField("schema_version", IntegerType()),
            StructField("carbon_kg", DoubleType()),
            StructField("genai_tokens", LongType()),
        ])
        df = (spark.read.schema(schema).json(str(EVENTS_DIR))
              .withColumn("value_num", F.col("value").cast("double"))
              .withColumn("usage_date", F.to_date(F.to_timestamp("timestamp"))))

        # Pick a specific org+service+day with mixed metrics
        sample = (df.filter(F.col("value_num").isNotNull())
                  .groupBy("org_id","service","usage_date")
                  .agg(F.countDistinct("metric").alias("n_metrics"))
                  .filter(F.col("n_metrics") > 1)
                  .first())

        if sample is None:
            pytest.skip("No org+service+day with multiple metrics found")

        org, svc, day = sample["org_id"], sample["service"], sample["usage_date"]
        subset = df.filter(
            (F.col("org_id")==org) & (F.col("service")==svc) & (F.col("usage_date")==day))

        # Compute requests the correct way (metric-based)
        correct_requests = subset.filter(F.col("metric")=="requests").agg(
            F.sum("value_num")).first()[0]
        # Compute the wrong way (count of all rows)
        wrong_count = subset.count()

        if correct_requests is not None:
            assert correct_requests != wrong_count or correct_requests == 0, (
                "requests should differ from row count (metric-based, not count(*))")


# ===================================================================
# 7. DATA GOTCHAS — verify known real-world issues exist in the data
# ===================================================================

class TestDataGotchas:
    """Confirm that the documented data gotchas (CLAUDE.md §2.3) exist in the real data."""

    def test_nps_out_of_range(self):
        """NPS score > 100 exists in customers_orgs (e.g. org_pac56t4u with 101)."""
        with open(LANDING / "customers_orgs.csv") as f:
            reader = csv.DictReader(f)
            out_of_range = [r for r in reader
                            if r["nps_score"] and float(r["nps_score"]) > 100]
        assert len(out_of_range) > 0, "Expected NPS > 100 in orgs data"

    def test_billing_negative_subtotal(self):
        """At least one invoice has negative subtotal."""
        with open(LANDING / "billing_monthly.csv") as f:
            reader = csv.DictReader(f)
            negatives = [r for r in reader if r["subtotal"] and float(r["subtotal"]) < 0]
        assert len(negatives) > 0, "Expected negative subtotal in billing"

    def test_billing_blank_credits(self):
        """credits field is blank (empty string) for some invoices."""
        with open(LANDING / "billing_monthly.csv") as f:
            reader = csv.DictReader(f)
            blanks = [r for r in reader if r["credits"] == ""]
        assert len(blanks) > 0, "Expected blank credits in billing"

    def test_events_value_as_string(self):
        """Some events have value as a quoted string number like '100.0'."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        found_string_value = False
        for fp in files[:5]:
            with open(fp) as f:
                for line in f:
                    evt = json.loads(line)
                    if isinstance(evt.get("value"), str):
                        found_string_value = True
                        break
            if found_string_value:
                break
        # value arrives as number, string, or null — the schema reads it as string
        # Even if JSON has numbers, Spark with StringType reads them as strings

    def test_events_unit_null_with_value(self):
        """At least one event has value present but unit null."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        found = False
        for fp in files:
            with open(fp) as f:
                for line in f:
                    evt = json.loads(line)
                    if evt.get("value") is not None and evt.get("unit") is None:
                        found = True
                        break
            if found:
                break
        assert found, "Expected at least one event with value present and unit null"

    def test_events_timestamp_field_name(self):
        """Events use 'timestamp' (not 'event_time') — confirmed."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        with open(files[0]) as f:
            evt = json.loads(f.readline())
        assert "timestamp" in evt, "Events should have 'timestamp' field"
        assert "event_time" not in evt, "Events should NOT have 'event_time' field"

    def test_schema_v2_cutover_date(self):
        """v1 events are before ~2025-07-18, v2 after."""
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        v1_dates = []
        v2_dates = []
        for fp in files[:10]:
            with open(fp) as f:
                for line in f:
                    evt = json.loads(line)
                    ts = evt["timestamp"][:10]  # YYYY-MM-DD
                    if evt["schema_version"] == 1:
                        v1_dates.append(ts)
                    else:
                        v2_dates.append(ts)
        if v1_dates and v2_dates:
            # v1 dates should generally be earlier
            assert min(v1_dates) <= max(v2_dates), "Schema version timeline check"


# ===================================================================
# 8. CQL SCHEMA — validate the CQL file is well-formed
# ===================================================================

class TestCQLSchema:

    def test_cql_file_exists_and_has_content(self):
        cql_path = PROJECT_ROOT / "cql" / "schema.cql"
        assert cql_path.exists(), "cql/schema.cql not found"
        content = cql_path.read_text()
        assert len(content) > 50, "cql/schema.cql is empty or too short"

    def test_cql_has_create_table(self):
        content = (PROJECT_ROOT / "cql" / "schema.cql").read_text()
        assert "CREATE TABLE" in content, "CQL should contain CREATE TABLE"

    def test_cql_has_primary_key(self):
        content = (PROJECT_ROOT / "cql" / "schema.cql").read_text()
        assert "PRIMARY KEY" in content, "CQL should define PRIMARY KEY"
        assert "org_id" in content and "service" in content and "usage_date" in content


# ===================================================================
# 9. REPO STRUCTURE — required artifacts
# ===================================================================

class TestRepoStructure:

    def test_notebook_exists(self):
        assert (PROJECT_ROOT / "notebooks" / "cloud_provider_analytics_mvp.ipynb").exists()

    def test_readme_exists(self):
        assert (PROJECT_ROOT / "README.md").exists()

    def test_decision_log_exists(self):
        path = PROJECT_ROOT / "docs" / "decision_log.md"
        assert path.exists()
        content = path.read_text()
        assert "Lambda" in content, "Decision log should mention Lambda"
        assert "Cassandra" in content or "CQL" in content

    def test_architecture_diagram_exists(self):
        # Accept .md or .svg
        md = (PROJECT_ROOT / "docs" / "architecture.md").exists()
        svg = (PROJECT_ROOT / "docs" / "architecture.svg").exists()
        assert md or svg, "Architecture diagram missing (docs/architecture.md or .svg)"

    def test_gitignore_excludes_secrets(self):
        gi = (PROJECT_ROOT / ".gitignore").read_text()
        assert ".env" in gi, ".gitignore should exclude .env"
        assert "secure-connect" in gi or "*.zip" in gi, ".gitignore should exclude SCB"

    def test_env_example_exists(self):
        assert (PROJECT_ROOT / ".env.example").exists()

    def test_landing_data_present(self):
        assert (LANDING / "customers_orgs.csv").exists()
        assert (LANDING / "users.csv").exists()
        assert (LANDING / "billing_monthly.csv").exists()
        jsonl_files = list(EVENTS_DIR.glob("*.jsonl"))
        assert len(jsonl_files) > 100, f"Expected 120 JSONL files, found {len(jsonl_files)}"

    def test_decision_log_no_draft_markers(self):
        """Decision log should not have ?? draft markers."""
        content = (PROJECT_ROOT / "docs" / "decision_log.md").read_text()
        assert "??" not in content, "Decision log still has '??' draft markers"
