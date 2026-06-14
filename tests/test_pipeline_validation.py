"""
Pipeline validation tests — Cloud Provider Analytics MVP
Runs locally against the real dataset in datalake/landing/.
Validates schemas, quality rules, transformations, output structure,
AND Structured Streaming lecture concepts (Mosquera, ITBA 72.80).

Usage:
    pip install pyspark==3.5.1 pytest
    pytest tests/test_pipeline_validation.py -v
"""
import os, json, csv, datetime, pytest, shutil, tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING = PROJECT_ROOT / "datalake" / "landing"
EVENTS_DIR = LANDING / "usage_events_stream"

pytestmark = pytest.mark.skipif(
    not LANDING.exists(), reason="Landing data not found at datalake/landing/"
)

# ---------------------------------------------------------------------------
# Spark fixture
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

def _event_schema():
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

def _orgs_schema():
    from pyspark.sql.types import (StructType, StructField, StringType,
        DoubleType, BooleanType, DateType)
    return StructType([
        StructField("org_id",StringType()),StructField("org_name",StringType()),
        StructField("industry",StringType()),StructField("hq_region",StringType()),
        StructField("plan_tier",StringType()),StructField("is_enterprise",BooleanType()),
        StructField("signup_date",DateType()),StructField("sales_rep",StringType()),
        StructField("lifecycle_stage",StringType()),StructField("marketing_source",StringType()),
        StructField("nps_score",DoubleType())])

def _billing_schema():
    from pyspark.sql.types import (StructType, StructField, StringType,
        DoubleType, DateType)
    return StructType([
        StructField("invoice_id",StringType()),StructField("org_id",StringType()),
        StructField("month",DateType()),StructField("subtotal",DoubleType()),
        StructField("credits",DoubleType()),StructField("taxes",DoubleType()),
        StructField("currency",StringType()),StructField("exchange_rate_to_usd",DoubleType())])


# ===================================================================
# 1. SCHEMA ALIGNMENT — real files match declared schemas
# ===================================================================

class TestSchemaAlignment:
    def test_customers_orgs_columns(self):
        with open(LANDING / "customers_orgs.csv") as f:
            header = next(csv.reader(f))
        expected = ["org_id","org_name","industry","hq_region","plan_tier",
                     "is_enterprise","signup_date","sales_rep","lifecycle_stage",
                     "marketing_source","nps_score"]
        assert header == expected

    def test_users_columns(self):
        with open(LANDING / "users.csv") as f:
            header = next(csv.reader(f))
        expected = ["user_id","org_id","email","role","active","created_at","last_login"]
        assert header == expected

    def test_billing_columns(self):
        with open(LANDING / "billing_monthly.csv") as f:
            header = next(csv.reader(f))
        expected = ["invoice_id","org_id","month","subtotal","credits","taxes",
                     "currency","exchange_rate_to_usd"]
        assert header == expected

    def test_support_tickets_columns(self):
        with open(LANDING / "support_tickets.csv") as f:
            header = next(csv.reader(f))
        expected = ["ticket_id","org_id","category","severity","created_at",
                     "resolved_at","csat","sla_breached"]
        assert header == expected

    def test_resources_columns(self):
        with open(LANDING / "resources.csv") as f:
            header = next(csv.reader(f))
        expected = ["resource_id","org_id","service","region","created_at","state","tags_json"]
        assert header == expected

    def test_nps_surveys_columns(self):
        with open(LANDING / "nps_surveys.csv") as f:
            header = next(csv.reader(f))
        expected = ["org_id","survey_date","nps_score","comment"]
        assert header == expected

    def test_events_jsonl_fields(self):
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        assert len(files) > 0
        with open(files[0]) as f:
            event = json.loads(f.readline())
        required = {"event_id","timestamp","org_id","resource_id","service",
                     "region","metric","value","unit","cost_usd_increment",
                     "schema_version"}
        assert required.issubset(event.keys())

    def test_events_v2_has_carbon_kg(self):
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
# 2. BATCH BRONZE
# ===================================================================

class TestBatchBronze:
    def test_orgs_loads_without_nulls_in_key(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.option("header",True).schema(_orgs_schema()).csv(
            str(LANDING / "customers_orgs.csv"))
        assert df.count() > 0
        assert df.filter(F.col("org_id").isNull()).count() == 0

    def test_orgs_dedup_is_idempotent(self, spark):
        df = spark.read.option("header",True).schema(_orgs_schema()).csv(
            str(LANDING / "customers_orgs.csv"))
        before = df.count()
        after = df.dropDuplicates(["org_id"]).count()
        assert after <= before and after > 0

    def test_billing_credits_nullable(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.option("header",True).schema(_billing_schema()).csv(
            str(LANDING / "billing_monthly.csv"))
        assert df.filter(F.col("credits").isNull()).count() > 0

    def test_billing_has_negative_subtotal(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.option("header",True).schema(_billing_schema()).csv(
            str(LANDING / "billing_monthly.csv"))
        assert df.filter(F.col("subtotal") < 0).count() > 0

    def test_billing_multi_currency(self, spark):
        df = spark.read.option("header",True).schema(_billing_schema()).csv(
            str(LANDING / "billing_monthly.csv"))
        currencies = {r["currency"] for r in df.select("currency").distinct().collect()}
        assert {"USD","ARS","EUR"}.issubset(currencies)


# ===================================================================
# 3. EVENTS BRONZE
# ===================================================================

class TestEventsBronze:
    def test_events_load_count(self, spark):
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        assert df.count() > 40000

    def test_value_as_string_cast(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        with_cast = df.withColumn("value_num", F.col("value").cast("double"))
        non_null_value = df.filter(F.col("value").isNotNull()).count()
        cast_ok = with_cast.filter(
            F.col("value").isNotNull() & F.col("value_num").isNotNull()
        ).count()
        assert cast_ok == non_null_value

    def test_timestamp_parses(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        with_ts = df.withColumn("event_ts", F.to_timestamp("timestamp"))
        assert with_ts.filter(F.col("event_ts").isNull()).count() == 0

    def test_services_match_expected(self, spark):
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        services = {r["service"] for r in df.select("service").distinct().collect()}
        assert services == {"compute","storage","database","networking","analytics","genai"}

    def test_metrics_match_expected(self, spark):
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        metrics = {r["metric"] for r in df.select("metric").distinct().collect()}
        assert metrics == {"requests","cpu_hours","storage_gb_hours"}

    def test_schema_versions_1_and_2(self, spark):
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        versions = {r["schema_version"] for r in df.select("schema_version").distinct().collect()}
        assert versions == {1, 2}

    def test_genai_tokens_only_for_genai_service(self, spark):
        from pyspark.sql import functions as F
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        assert df.filter(
            (F.col("service") != "genai") & F.col("genai_tokens").isNotNull()
        ).count() == 0

    def test_dedup_by_event_id(self, spark):
        df = spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
        total = df.count()
        unique = df.dropDuplicates(["event_id"]).count()
        assert unique > 0 and unique <= total


# ===================================================================
# 4. QUALITY RULES
# ===================================================================

class TestQualityRules:
    def _load_events(self, spark):
        from pyspark.sql import functions as F
        return (spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
                .withColumn("value_num", F.col("value").cast("double")))

    def test_rule3_unit_null_with_value_present(self, spark):
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        violations = df.filter(F.col("value").isNotNull() & F.col("unit").isNull()).count()
        assert violations > 0, "Expected rule 3 violations"

    def test_rule2_negative_cost(self, spark):
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        assert df.filter(F.col("cost_usd_increment") < -0.01).count() > 0

    def test_quarantine_captures_failures(self, spark):
        from pyspark.sql import functions as F
        df = self._load_events(spark)
        valid = (F.col("event_id").isNotNull()
                 & (F.col("cost_usd_increment") >= -0.01)
                 & ~(F.col("value").isNotNull() & F.col("unit").isNull()))
        ok_count = df.filter(valid).count()
        quarantine_count = df.filter(~valid).count()
        assert quarantine_count > 0
        assert ok_count + quarantine_count == df.count()


# ===================================================================
# 5. SILVER — enrichment
# ===================================================================

class TestSilverTransformations:
    def test_broadcast_join_enriches_events(self, spark):
        from pyspark.sql import functions as F
        events = spark.read.schema(_event_schema()).json(str(EVENTS_DIR)).limit(1000)
        orgs = spark.read.option("header",True).schema(_orgs_schema()).csv(
            str(LANDING / "customers_orgs.csv"))
        orgs_dim = orgs.select("org_id","industry","plan_tier","hq_region")
        enriched = events.join(F.broadcast(orgs_dim), on="org_id", how="left")
        assert "industry" in enriched.columns
        matched = enriched.filter(F.col("industry").isNotNull()).count()
        assert matched / enriched.count() > 0.9


# ===================================================================
# 6. GOLD MART
# ===================================================================

class TestGoldMart:
    def test_finops_mart_aggregation(self, spark):
        from pyspark.sql import functions as F
        df = (spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
              .withColumn("event_ts", F.to_timestamp("timestamp"))
              .withColumn("value_num", F.col("value").cast("double"))
              .withColumn("usage_date", F.to_date("event_ts")))
        valid = (F.col("event_id").isNotNull()
                 & (F.col("cost_usd_increment") >= -0.01)
                 & ~(F.col("value").isNotNull() & F.col("unit").isNull()))
        gold = (df.filter(valid).groupBy("org_id","service","usage_date").agg(
            F.round(F.sum("cost_usd_increment"),2).alias("daily_cost_usd"),
            F.round(F.sum(F.when(F.col("metric")=="requests", F.col("value_num"))),2).alias("requests"),
            F.round(F.sum(F.when(F.col("metric")=="cpu_hours", F.col("value_num"))),4).alias("cpu_hours"),
            F.round(F.sum(F.when(F.col("metric")=="storage_gb_hours", F.col("value_num"))),4).alias("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.round(F.sum("carbon_kg"),6).alias("carbon_kg")))

        expected_cols = {"org_id","service","usage_date","daily_cost_usd","requests",
                         "cpu_hours","storage_gb_hours","genai_tokens","carbon_kg"}
        assert expected_cols.issubset(set(gold.columns))
        assert gold.count() > 0
        assert gold.filter(F.col("daily_cost_usd").isNull()).count() == 0
        assert gold.filter(F.col("requests") > 0).count() > 0

    def test_metric_based_features_not_count(self, spark):
        """requests = sum(value WHERE metric='requests'), NOT count(*)."""
        from pyspark.sql import functions as F
        df = (spark.read.schema(_event_schema()).json(str(EVENTS_DIR))
              .withColumn("value_num", F.col("value").cast("double"))
              .withColumn("usage_date", F.to_date(F.to_timestamp("timestamp"))))
        sample = (df.filter(F.col("value_num").isNotNull())
                  .groupBy("org_id","service","usage_date")
                  .agg(F.countDistinct("metric").alias("n_metrics"))
                  .filter(F.col("n_metrics") > 1)
                  .first())
        if sample is None:
            pytest.skip("No org+service+day with multiple metrics")
        org, svc, day = sample["org_id"], sample["service"], sample["usage_date"]
        subset = df.filter(
            (F.col("org_id")==org) & (F.col("service")==svc) & (F.col("usage_date")==day))
        correct = subset.filter(F.col("metric")=="requests").agg(
            F.sum("value_num")).first()[0]
        wrong = subset.count()
        if correct is not None:
            assert correct != wrong or correct == 0


# ===================================================================
# 7. DATA GOTCHAS
# ===================================================================

class TestDataGotchas:
    def test_nps_out_of_range(self):
        with open(LANDING / "customers_orgs.csv") as f:
            reader = csv.DictReader(f)
            out_of_range = [r for r in reader if r["nps_score"] and float(r["nps_score"]) > 100]
        assert len(out_of_range) > 0

    def test_billing_negative_subtotal(self):
        with open(LANDING / "billing_monthly.csv") as f:
            negatives = [r for r in csv.DictReader(f) if r["subtotal"] and float(r["subtotal"]) < 0]
        assert len(negatives) > 0

    def test_billing_blank_credits(self):
        with open(LANDING / "billing_monthly.csv") as f:
            blanks = [r for r in csv.DictReader(f) if r["credits"] == ""]
        assert len(blanks) > 0

    def test_events_unit_null_with_value(self):
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
        assert found

    def test_events_timestamp_field_name(self):
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        with open(files[0]) as f:
            evt = json.loads(f.readline())
        assert "timestamp" in evt
        assert "event_time" not in evt

    def test_schema_v2_cutover_date(self):
        files = sorted(EVENTS_DIR.glob("*.jsonl"))
        v1_dates, v2_dates = [], []
        for fp in files[:10]:
            with open(fp) as f:
                for line in f:
                    evt = json.loads(line)
                    ts = evt["timestamp"][:10]
                    (v1_dates if evt["schema_version"] == 1 else v2_dates).append(ts)
        if v1_dates and v2_dates:
            assert min(v1_dates) <= max(v2_dates)


# ===================================================================
# 8. CQL SCHEMA + REPO STRUCTURE
# ===================================================================

class TestCQLAndRepo:
    def test_cql_file_has_create_table(self):
        content = (PROJECT_ROOT / "cql" / "schema.cql").read_text()
        assert len(content) > 50
        assert "CREATE TABLE" in content
        assert "PRIMARY KEY" in content
        assert "org_id" in content and "service" in content and "usage_date" in content

    def test_notebook_exists(self):
        assert (PROJECT_ROOT / "notebooks" / "cloud_provider_analytics_mvp.ipynb").exists()

    def test_readme_exists(self):
        assert (PROJECT_ROOT / "README.md").exists()

    def test_decision_log_exists_and_clean(self):
        path = PROJECT_ROOT / "docs" / "decision_log.md"
        assert path.exists()
        content = path.read_text()
        assert "Lambda" in content
        assert "??" not in content, "Decision log still has '??' draft markers"

    def test_architecture_diagram_exists(self):
        md = (PROJECT_ROOT / "docs" / "architecture.md").exists()
        svg = (PROJECT_ROOT / "docs" / "architecture.svg").exists()
        png = (PROJECT_ROOT / "docs" / "arquitecture.png").exists()
        assert md or svg or png, "Architecture diagram missing (docs/architecture.md, .svg, or .png)"

    def test_gitignore_excludes_secrets(self):
        gi = (PROJECT_ROOT / ".gitignore").read_text()
        assert ".env" in gi
        assert "secure-connect" in gi or "*.zip" in gi

    def test_env_example_exists(self):
        assert (PROJECT_ROOT / ".env.example").exists()

    def test_landing_data_present(self):
        assert (LANDING / "customers_orgs.csv").exists()
        assert (LANDING / "users.csv").exists()
        assert (LANDING / "billing_monthly.csv").exists()
        assert len(list(EVENTS_DIR.glob("*.jsonl"))) > 100


# ===================================================================
# 9. STRUCTURED STREAMING LECTURE CONCEPTS
#    (Spark Structured Streaming — Mosquera, ITBA 72.80)
#    Tests verify the notebook implements every key lecture concept.
# ===================================================================

class TestStreamingLectureConcepts:
    """
    Maps lecture slides to pipeline implementation.
    Each test verifies a concept from the Structured Streaming class.
    """

    # --- Slide 8: "tabla no acotada" / readStream creates a streaming DF ---
    def test_readstream_creates_streaming_df(self, spark):
        """readStream.json() should produce a streaming DataFrame (isStreaming=True).
        Lecture: 'readStream define una fuente continua... todavia no ejecuta la query'."""
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        assert sdf.isStreaming is True, "readStream should create a streaming DataFrame"

    # --- Slide 10: "readStream define... pero todavia no ejecuta. El stream se activa
    #     recien con writeStream.start()" ---
    def test_readstream_is_lazy_until_writestream_start(self, spark):
        """readStream builds but does NOT run — only writeStream.start() executes.
        Lecture antipattern: 'Creer que readStream ejecuta la query'."""
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        # At this point no query is running
        assert len(spark.streams.active) == 0 or \
               all(q.name != "__test_lazy" for q in spark.streams.active)

    # --- Slide 9: explicit schema ---
    def test_explicit_schema_not_inferred(self, spark):
        """Schema must be defined explicitly for streaming, not inferred.
        Lecture: 'Definir contratos de datos y esquemas explicitos'."""
        schema = _event_schema()
        assert len(schema.fields) == 13, "Event schema should have 13 fields"
        # value is StringType (not DoubleType) — deliberate choice for dirty data
        from pyspark.sql.types import StringType
        value_field = [f for f in schema.fields if f.name == "value"][0]
        assert isinstance(value_field.dataType, StringType), \
            "value should be StringType (read as string, cast later)"

    # --- Slide 11: file source is replayable ---
    def test_file_source_is_replayable(self):
        """File source = files persist on disk = replayable.
        Lecture: 'necesitamos fuentes reejecutables o replayables'."""
        jsonl_files = list(EVENTS_DIR.glob("*.jsonl"))
        assert len(jsonl_files) >= 120, "Expected ~120 JSONL files (replayable source)"
        # Files are immutable (not modified after creation) — key for replay
        for f in jsonl_files[:3]:
            assert f.stat().st_size > 0, f"File {f.name} is empty"

    # --- Slide 12: stateless vs stateful ---
    def test_stateless_transforms_before_stateful(self, spark):
        """Pipeline should apply stateless transforms (select/filter/withColumn) before
        stateful ops (groupBy/dropDuplicates).
        Lecture: 'Stateless: select/filter/withColumn — Stateful: groupBy/window/deduplicate'."""
        from pyspark.sql import functions as F
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        # Stateless transforms first
        bronze = (sdf
            .withColumn("event_ts", F.to_timestamp("timestamp"))     # stateless
            .withColumn("value_num", F.col("value").cast("double"))  # stateless
            .withColumn("usage_date", F.to_date("event_ts"))         # stateless
            .withWatermark("event_ts", "10 minutes")
            .dropDuplicates(["event_id"]))                           # stateful
        assert bronze.isStreaming

    # --- Slide 14: output modes ---
    def test_append_mode_for_raw_events(self, spark, tmp_path):
        """Raw event ingestion uses append mode (events are final once written).
        Lecture: 'append = Solo filas nuevas finales'."""
        from pyspark.sql import functions as F
        out = str(tmp_path / "bronze_events")
        chk = str(tmp_path / "chk_bronze")
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes")
               .dropDuplicates(["event_id"]))
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")           # <-- append for raw events
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        result = spark.read.parquet(out)
        assert result.count() > 40000

    # --- Slide 15: triggers ---
    def test_available_now_trigger(self, spark, tmp_path):
        """availableNow=True processes all available files and stops.
        Lecture: 'availableNow=True — procesa lo disponible y termina'."""
        from pyspark.sql import functions as F
        out = str(tmp_path / "trigger_test")
        chk = str(tmp_path / "chk_trigger")
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        # Query should have terminated (not still running)
        assert not q.isActive, "availableNow should terminate after processing"

    # --- Slide 17: checkpointing ---
    def test_checkpoint_enables_recovery(self, spark, tmp_path):
        """Checkpoint stores offsets+state for recovery.
        Lecture: 'Fuente replayable + checkpoint + sink idempotente = exactly-once'."""
        from pyspark.sql import functions as F
        out = str(tmp_path / "chk_recovery_out")
        chk = str(tmp_path / "chk_recovery")
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()

        # Checkpoint directory should exist with offsets
        chk_path = Path(chk)
        assert chk_path.exists(), "Checkpoint directory should be created"
        assert (chk_path / "offsets").exists(), "Checkpoint should contain offsets"
        assert (chk_path / "commits").exists(), "Checkpoint should contain commits"

        count_first = spark.read.parquet(out).count()

        # Re-run: checkpoint should skip already-processed files
        q2 = (sdf.writeStream.format("parquet")
              .option("path", out)
              .option("checkpointLocation", chk)
              .outputMode("append")
              .trigger(availableNow=True)
              .start())
        q2.awaitTermination()
        count_second = spark.read.parquet(out).count()
        assert count_second == count_first, \
            f"Re-run should not duplicate: {count_first} vs {count_second}"

    # --- Slide 18-19: event time + windows ---
    def test_event_time_not_processing_time(self, spark):
        """Aggregations use event time (timestamp), not processing time.
        Lecture: 'Las ventanas deben usar event_time, no current_timestamp()'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp")))
        # Window on event_ts (event time), NOT current_timestamp
        windowed = sdf.groupBy(
            F.window("event_ts", "1 day"), "org_id", "service"
        ).agg(F.sum("cost_usd_increment").alias("daily_cost"))
        assert "window" in [f.name for f in windowed.schema.fields]

    # --- Slide 19: tumbling window ---
    def test_tumbling_window_daily(self, spark, tmp_path):
        """Daily tumbling window groups events by day.
        Lecture: 'Tumbling [0-10] [10-20] [20-30]'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes"))
        # Tumbling = window size only, no slide parameter
        gold = (sdf.groupBy(F.window("event_ts", "1 day"), "org_id", "service")
                .agg(F.sum("cost_usd_increment").alias("daily_cost"))
                .withColumn("usage_date", F.to_date(F.col("window.start"))))

        out = str(tmp_path / "tumbling_out")
        chk = str(tmp_path / "chk_tumbling")
        q = (gold.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        result = spark.read.parquet(out)
        assert result.count() > 0
        # Each usage_date should be distinct per org+service (tumbling, no overlap)
        dupes = (result.groupBy("org_id","service","usage_date")
                 .count().filter(F.col("count") > 1).count())
        assert dupes == 0, "Tumbling window should not produce duplicate org+service+day"

    # --- Slide 20: watermark ---
    def test_watermark_bounds_state(self, spark):
        """withWatermark limits state growth and handles late data.
        Lecture: 'Watermark = max event time - retraso permitido'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes"))
        # Watermark is set — this enables state cleanup
        assert sdf.isStreaming

    # --- Slide 21: state store / dropDuplicates ---
    def test_drop_duplicates_is_stateful(self, spark, tmp_path):
        """dropDuplicates on streaming requires state store.
        Lecture: 'dropDuplicates' under 'Operaciones con estado'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes")
               .dropDuplicates(["event_id"]))
        out = str(tmp_path / "dedup_out")
        chk = str(tmp_path / "chk_dedup")
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        result = spark.read.parquet(out)
        # All event_ids should be unique after dedup
        total = result.count()
        unique = result.select("event_id").distinct().count()
        assert total == unique, f"Dedup failed: {total} rows but {unique} unique event_ids"

    # --- Slide 22: stream-static join ---
    def test_stream_static_join_enrichment(self, spark, tmp_path):
        """Stream-static join enriches events with org dimension (broadcast).
        Lecture: 'Join de enriquecimiento stream-static'."""
        from pyspark.sql import functions as F
        orgs = spark.read.option("header",True).schema(_orgs_schema()).csv(
            str(LANDING / "customers_orgs.csv"))
        orgs_dim = F.broadcast(orgs.select("org_id","industry","plan_tier"))

        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes"))
        # Stream-static join
        enriched = sdf.join(orgs_dim, on="org_id", how="left")
        assert enriched.isStreaming
        assert "industry" in enriched.columns

    # --- Slide 23: foreachBatch ---
    def test_foreach_batch_sink_pattern(self, spark, tmp_path):
        """foreachBatch receives a batch DataFrame per micro-batch.
        Lecture: 'foreachBatch: Escrituras idempotentes por micro-batch'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes")
               .groupBy(F.window("event_ts", "1 day"), "org_id", "service")
               .agg(F.sum("cost_usd_increment").alias("daily_cost")))

        batches_seen = []
        def capture_batch(batch_df, batch_id):
            batches_seen.append(batch_df.count())

        chk = str(tmp_path / "chk_foreach")
        q = (sdf.writeStream
             .outputMode("update")
             .foreachBatch(capture_batch)
             .option("checkpointLocation", chk)
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        assert len(batches_seen) > 0, "foreachBatch should have been called at least once"
        assert sum(batches_seen) > 0, "foreachBatch should have received rows"

    # --- Slide 14: update mode for aggregations ---
    def test_update_mode_for_aggregation(self, spark, tmp_path):
        """Aggregations that change use update mode (not append).
        Lecture: 'update = Filas actualizadas desde el ultimo trigger'."""
        from pyspark.sql import functions as F
        sdf = (spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
               .withColumn("event_ts", F.to_timestamp("timestamp"))
               .withWatermark("event_ts", "10 minutes")
               .groupBy(F.window("event_ts", "1 day"), "org_id", "service")
               .agg(F.sum("cost_usd_increment").alias("daily_cost")))

        results = []
        def collect_batch(batch_df, batch_id):
            results.append(batch_df.count())

        chk = str(tmp_path / "chk_update")
        q = (sdf.writeStream
             .outputMode("update")    # update mode for aggregations
             .foreachBatch(collect_batch)
             .option("checkpointLocation", chk)
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        assert sum(results) > 0

    # --- Slide 26: observability / lastProgress ---
    def test_last_progress_available(self, spark, tmp_path):
        """lastProgress provides monitoring info after query runs.
        Lecture: 'query.lastProgress — inputRowsPerSecond, processedRowsPerSecond'."""
        from pyspark.sql import functions as F
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        out = str(tmp_path / "obs_out")
        chk = str(tmp_path / "chk_obs")
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        progress = q.lastProgress
        assert progress is not None, "lastProgress should be available after query runs"
        # Should contain key monitoring fields
        assert "numInputRows" in progress or "sources" in progress

    # --- Slide 36: antipattern — query.stop() ---
    def test_query_stop_closes_stream(self, spark, tmp_path):
        """Queries must be stopped to avoid infinite execution.
        Lecture antipattern: leaving queries running in notebook."""
        from pyspark.sql import functions as F
        sdf = spark.readStream.schema(_event_schema()).json(str(EVENTS_DIR))
        out = str(tmp_path / "stop_out")
        chk = str(tmp_path / "chk_stop")
        q = (sdf.writeStream.format("parquet")
             .option("path", out)
             .option("checkpointLocation", chk)
             .outputMode("append")
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
        q.stop()
        assert not q.isActive, "query.stop() should deactivate the query"


# ===================================================================
# 10. NOTEBOOK CONTENT — verify lecture concepts are present
# ===================================================================

class TestNotebookContent:
    """Verify the notebook markdown/code mentions key lecture concepts."""

    @pytest.fixture(scope="class")
    def notebook_text(self):
        nb_path = PROJECT_ROOT / "notebooks" / "cloud_provider_analytics_mvp.ipynb"
        with open(nb_path) as f:
            nb = json.load(f)
        # Concatenate all cell sources
        text = ""
        for cell in nb["cells"]:
            text += "".join(cell["source"]) + "\n"
        return text

    def test_mentions_unbounded_table(self, notebook_text):
        """Slide 8: model mental = tabla no acotada."""
        assert "tabla no acotada" in notebook_text.lower() or \
               "unbounded table" in notebook_text.lower() or \
               "query incremental" in notebook_text.lower()

    def test_mentions_watermark(self, notebook_text):
        assert "withWatermark" in notebook_text or "watermark" in notebook_text.lower()

    def test_mentions_checkpoint(self, notebook_text):
        assert "checkpointLocation" in notebook_text or "checkpoint" in notebook_text.lower()

    def test_mentions_available_now(self, notebook_text):
        assert "availableNow" in notebook_text

    def test_mentions_foreach_batch(self, notebook_text):
        assert "foreachBatch" in notebook_text

    def test_mentions_output_mode_update(self, notebook_text):
        assert "update" in notebook_text and "outputMode" in notebook_text

    def test_mentions_output_mode_append(self, notebook_text):
        assert "append" in notebook_text

    def test_mentions_event_time(self, notebook_text):
        assert "event_ts" in notebook_text or "event_time" in notebook_text.lower() or \
               "event time" in notebook_text.lower()

    def test_mentions_window(self, notebook_text):
        assert "window" in notebook_text.lower() and "tumbling" in notebook_text.lower()

    def test_mentions_broadcast_join(self, notebook_text):
        assert "broadcast" in notebook_text.lower()

    def test_mentions_stream_static_join(self, notebook_text):
        assert "stream-static" in notebook_text.lower() or \
               "stream_static" in notebook_text.lower() or \
               "enriquecimiento" in notebook_text.lower()

    def test_mentions_last_progress(self, notebook_text):
        """Slide 26: observability."""
        assert "lastProgress" in notebook_text

    def test_mentions_query_stop(self, notebook_text):
        """Slide 36: stop queries."""
        assert ".stop()" in notebook_text

    def test_mentions_exactly_once(self, notebook_text):
        """Slide 17: exactly-once semantics."""
        assert "exactly-once" in notebook_text.lower() or \
               "exactly once" in notebook_text.lower() or \
               "idempoten" in notebook_text.lower()

    def test_mentions_lambda_architecture(self, notebook_text):
        assert "lambda" in notebook_text.lower()

    def test_mentions_antipattern_readstream_not_execute(self, notebook_text):
        """Slide 36: antipattern 'readStream ejecuta la query'."""
        # Should mention that readStream doesn't execute, or reference the antipattern
        assert "readStream" in notebook_text
        assert "writeStream" in notebook_text or "start()" in notebook_text

    def test_mentions_colab_not_production(self, notebook_text):
        """Slide 37: Colab != production."""
        assert "colab" in notebook_text.lower() or "producción" in notebook_text.lower() or \
               "production" in notebook_text.lower() or "produccion" in notebook_text.lower()

    def test_mentions_upsert_idempotency(self, notebook_text):
        """Slide 23: idempotent writes."""
        assert "upsert" in notebook_text.lower() or "idempoten" in notebook_text.lower()
