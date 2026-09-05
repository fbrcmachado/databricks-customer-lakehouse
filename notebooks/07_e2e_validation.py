# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Validation
# MAGIC
# MAGIC Final orchestration gate. This notebook does not transform business data.
# MAGIC It verifies that all persisted layer states and published datasets describe
# MAGIC one coherent end-to-end data-product state.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
dbutils.widgets.text("expected_file_count", "50")

CATALOG = dbutils.widgets.get("catalog").strip()
EXPECTED_FILE_COUNT = int(
    dbutils.widgets.get("expected_file_count").strip()
)

INGESTION_MANIFEST_TABLE = (
    f"{CATALOG}.observability.ingestion_manifest"
)
SILVER_STATE_TABLE = (
    f"{CATALOG}.observability.transformation_state"
)
GOLD_STATE_TABLE = (
    f"{CATALOG}.observability.gold_publication_state"
)
ML_STATE_TABLE = (
    f"{CATALOG}.observability.ml_publication_state"
)
GOVERNANCE_STATE_TABLE = (
    f"{CATALOG}.observability.governance_state"
)

LATEST_HEALTH_VIEW = (
    f"{CATALOG}.observability.v_pipeline_health_latest"
)
GOVERNANCE_SUMMARY_TABLE = (
    f"{CATALOG}.governance.governance_summary"
)

BRONZE_TABLE = f"{CATALOG}.bronze.sales_raw"
SILVER_TABLE = f"{CATALOG}.silver.sales_transactions"
QUARANTINE_TABLE = (
    f"{CATALOG}.quarantine.sales_transactions_invalid"
)

DIM_CUSTOMER_TABLE = f"{CATALOG}.gold_bi.dim_customer"
FACT_SALES_TABLE = f"{CATALOG}.gold_bi.fact_sales"

CUSTOMER_FEATURES_TABLE = (
    f"{CATALOG}.gold_ml.customer_features"
)
CUSTOMER_SEGMENTS_TABLE = (
    f"{CATALOG}.gold_ml.customer_segments"
)

SILVER_LAYER_NAME = "silver_sales_transactions"
GOLD_LAYER_NAME = "gold_bi_star_schema"
ML_LAYER_NAME = "gold_ml_customer_segmentation"
GOVERNANCE_LAYER_NAME = "governance_as_code"

RUN_ID = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_"
    + uuid.uuid4().hex[:8]
)

# COMMAND ----------

def log_event(event: str, **details: object) -> None:
    print(
        json.dumps(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "run_id": RUN_ID,
                "event": event,
                **details,
            },
            ensure_ascii=False,
            default=str,
        )
    )


def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required E2E object is missing: {table_name}"
        )


def read_state(
    table_name: str,
    layer_name: str,
) -> dict[str, object]:
    require_table(table_name)

    rows = (
        spark.table(table_name)
        .where(F.col("layer_name") == layer_name)
        .limit(1)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            f"Required state row is missing: "
            f"{table_name} / {layer_name}"
        )

    return rows[0].asDict(recursive=True)


def assert_check(
    checks: dict[str, bool],
    name: str,
    condition: bool,
) -> None:
    checks[name] = bool(condition)

    if not condition:
        raise RuntimeError(
            f"E2E validation failed: {name}"
        )


# COMMAND ----------

log_event(
    "e2e_validation_started",
    catalog=CATALOG,
    expected_file_count=EXPECTED_FILE_COUNT,
)

required_objects = [
    INGESTION_MANIFEST_TABLE,
    SILVER_STATE_TABLE,
    GOLD_STATE_TABLE,
    ML_STATE_TABLE,
    GOVERNANCE_STATE_TABLE,
    LATEST_HEALTH_VIEW,
    GOVERNANCE_SUMMARY_TABLE,
    BRONZE_TABLE,
    SILVER_TABLE,
    QUARANTINE_TABLE,
    DIM_CUSTOMER_TABLE,
    FACT_SALES_TABLE,
    CUSTOMER_FEATURES_TABLE,
    CUSTOMER_SEGMENTS_TABLE,
]

for object_name in required_objects:
    require_table(object_name)

silver_state = read_state(
    SILVER_STATE_TABLE,
    SILVER_LAYER_NAME,
)
gold_state = read_state(
    GOLD_STATE_TABLE,
    GOLD_LAYER_NAME,
)
ml_state = read_state(
    ML_STATE_TABLE,
    ML_LAYER_NAME,
)
governance_state = read_state(
    GOVERNANCE_STATE_TABLE,
    GOVERNANCE_LAYER_NAME,
)

health_row = spark.table(LATEST_HEALTH_VIEW).first()

if health_row is None:
    raise RuntimeError(
        "Latest pipeline health view is empty."
    )

health = health_row.asDict(recursive=True)

governance_summary_row = (
    spark.table(GOVERNANCE_SUMMARY_TABLE)
    .orderBy(F.col("evaluated_at_utc").desc())
    .first()
)

if governance_summary_row is None:
    raise RuntimeError(
        "Governance summary is empty."
    )

governance_summary = governance_summary_row.asDict(
    recursive=True
)

active_source_files = (
    spark.table(INGESTION_MANIFEST_TABLE)
    .where(F.col("status") == "active")
    .select("source_file")
    .distinct()
    .count()
)

bronze_rows = spark.table(BRONZE_TABLE).count()
silver_rows = spark.table(SILVER_TABLE).count()
quarantine_rows = spark.table(QUARANTINE_TABLE).count()
fact_sales_rows = spark.table(FACT_SALES_TABLE).count()
dim_customer_rows = spark.table(DIM_CUSTOMER_TABLE).count()
customer_features_rows = (
    spark.table(CUSTOMER_FEATURES_TABLE).count()
)
customer_segments_rows = (
    spark.table(CUSTOMER_SEGMENTS_TABLE).count()
)

silver_signature = str(
    silver_state["input_signature"]
)
gold_signature = str(
    gold_state["input_signature"]
)
ml_signature = str(
    ml_state["input_signature"]
)
governance_signature = str(
    governance_state["input_signature"]
)

checks: dict[str, bool] = {}

assert_check(
    checks,
    "active_source_file_count",
    active_source_files == EXPECTED_FILE_COUNT,
)

assert_check(
    checks,
    "row_accounting",
    bronze_rows == silver_rows + quarantine_rows,
)

assert_check(
    checks,
    "observability_health",
    health["health_status"] == "HEALTHY",
)

assert_check(
    checks,
    "observability_matches_silver",
    health["source_input_signature"] == silver_signature,
)

assert_check(
    checks,
    "gold_matches_silver",
    gold_state["silver_input_signature"] == silver_signature,
)

assert_check(
    checks,
    "ml_matches_silver",
    ml_state["silver_input_signature"] == silver_signature,
)

assert_check(
    checks,
    "governance_state_pass",
    governance_state["overall_status"] == "PASS",
)

assert_check(
    checks,
    "governance_summary_pass",
    governance_summary["overall_status"] == "PASS",
)

assert_check(
    checks,
    "governance_summary_matches_silver",
    governance_summary["silver_input_signature"]
    == silver_signature,
)

assert_check(
    checks,
    "governance_summary_matches_gold",
    governance_summary["gold_input_signature"]
    == gold_signature,
)

assert_check(
    checks,
    "governance_summary_matches_ml",
    governance_summary["ml_input_signature"]
    == ml_signature,
)

assert_check(
    checks,
    "gold_fact_matches_silver",
    fact_sales_rows == silver_rows,
)

assert_check(
    checks,
    "ml_feature_segment_match",
    customer_features_rows == customer_segments_rows,
)

assert_check(
    checks,
    "customer_population_alignment",
    dim_customer_rows == customer_features_rows,
)

assert_check(
    checks,
    "mlflow_reference_present",
    bool(ml_state["mlflow_run_id"])
    and bool(ml_state["model_uri"]),
)

pipeline_signature_payload = {
    "silver_input_signature": silver_signature,
    "gold_input_signature": gold_signature,
    "ml_input_signature": ml_signature,
    "governance_signature": governance_signature,
    "active_source_files": active_source_files,
    "bronze_rows": bronze_rows,
    "silver_rows": silver_rows,
    "quarantine_rows": quarantine_rows,
    "fact_sales_rows": fact_sales_rows,
    "customer_features_rows": customer_features_rows,
    "customer_segments_rows": customer_segments_rows,
}

pipeline_signature = hashlib.sha256(
    json.dumps(
        pipeline_signature_payload,
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
).hexdigest()

result = {
    "run_id": RUN_ID,
    "status": "VALIDATED",
    "pipeline_status": "HEALTHY",
    "checks_passed": sum(checks.values()),
    "checks_total": len(checks),
    "active_source_files": active_source_files,
    "bronze_rows": bronze_rows,
    "silver_rows": silver_rows,
    "quarantine_rows": quarantine_rows,
    "fact_sales_rows": fact_sales_rows,
    "dim_customer_rows": dim_customer_rows,
    "customer_features_rows": customer_features_rows,
    "customer_segments_rows": customer_segments_rows,
    "silver_input_signature": silver_signature,
    "gold_input_signature": gold_signature,
    "ml_input_signature": ml_signature,
    "governance_signature": governance_signature,
    "mlflow_run_id": ml_state["mlflow_run_id"],
    "model_uri": ml_state["model_uri"],
    "pipeline_signature": pipeline_signature,
    "checks": checks,
}

log_event(
    "e2e_validation_completed",
    **result,
)

print(
    json.dumps(
        result,
        indent=2,
        ensure_ascii=False,
    )
)
print("Evidence level: ENVIRONMENT VALIDATED")

dbutils.notebook.exit(
    json.dumps(result)
)
