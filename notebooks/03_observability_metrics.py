# Databricks notebook source
# MAGIC %md
# MAGIC # Observability and Data Quality Metrics
# MAGIC
# MAGIC Produces a reproducible operational snapshot from Bronze, Silver,
# MAGIC Quarantine and the transformation state. Metrics are persisted in Delta
# MAGIC and are idempotent for the same Silver input signature plus observability contract.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
CATALOG = dbutils.widgets.get("catalog").strip()

BRONZE_TABLE = f"{CATALOG}.bronze.sales_raw"
INGESTION_MANIFEST_TABLE = f"{CATALOG}.observability.ingestion_manifest"
TRANSFORMATION_STATE_TABLE = f"{CATALOG}.observability.transformation_state"
SILVER_TABLE = f"{CATALOG}.silver.sales_transactions"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.sales_transactions_invalid"

SUMMARY_TABLE = f"{CATALOG}.observability.pipeline_metrics"
DQ_TABLE = f"{CATALOG}.observability.data_quality_metrics"

LATEST_HEALTH_VIEW = f"{CATALOG}.observability.v_pipeline_health_latest"
LATEST_DQ_VIEW = f"{CATALOG}.observability.v_data_quality_latest"

LAYER_NAME = "silver_sales_transactions"
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


def sql_string(value: str) -> str:
    return value.replace("'", "''")


def resolve_project_root() -> Path:
    context_path = (
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )

    if context_path.startswith("/Workspace/"):
        physical_notebook = Path(context_path)
    else:
        physical_notebook = Path("/Workspace") / context_path.lstrip("/")

    return physical_notebook.parent.parent


PROJECT_ROOT = resolve_project_root()
DATA_CONTRACT_PATH = PROJECT_ROOT / "config" / "data_contract.yaml"
OBSERVABILITY_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "observability_contract.yaml"
)

# COMMAND ----------

def load_yaml_with_hash(path: Path) -> tuple[dict, str]:
    if not path.exists():
        raise RuntimeError(f"Required contract not found: {path}")

    raw = path.read_bytes()
    parsed = yaml.safe_load(raw.decode("utf-8"))

    if not isinstance(parsed, dict):
        raise RuntimeError(f"Contract is not a YAML mapping: {path}")

    return parsed, hashlib.sha256(raw).hexdigest()


def create_tables() -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
            observation_signature STRING,
            source_input_signature STRING,
            observability_contract_sha256 STRING,
            observability_contract_version STRING,
            observed_run_id STRING,
            observed_at_utc TIMESTAMP,
            active_source_files BIGINT,
            bronze_files BIGINT,
            bronze_rows BIGINT,
            silver_rows BIGINT,
            quarantine_rows BIGINT,
            accounted_rows BIGINT,
            valid_pct DOUBLE,
            row_accounting_ok BOOLEAN,
            file_count_ok BOOLEAN,
            health_status STRING
        )
        USING DELTA
        COMMENT 'Pipeline-level operational and data-quality health snapshots'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
            observation_signature STRING,
            source_input_signature STRING,
            observed_run_id STRING,
            observed_at_utc TIMESTAMP,
            rule_code STRING,
            rule_name STRING,
            failed_rows BIGINT,
            failed_pct DOUBLE
        )
        USING DELTA
        COMMENT 'Rule-level data-quality metrics derived from Quarantine'
        """
    )


def read_silver_state() -> dict[str, object]:
    rows = (
        spark.table(TRANSFORMATION_STATE_TABLE)
        .where(F.col("layer_name") == LAYER_NAME)
        .limit(1)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            "Silver transformation state is missing. "
            "Run the Silver transformation successfully first."
        )

    state = rows[0].asDict(recursive=True)

    if not state.get("input_signature"):
        raise RuntimeError("Silver transformation state has no input_signature.")

    return state


def compute_observation_signature(
    source_input_signature: str,
    observability_contract_sha256: str,
) -> str:
    value = (
        f"{source_input_signature}:{observability_contract_sha256}"
    ).encode("utf-8")

    return hashlib.sha256(value).hexdigest()


def already_observed(observation_signature: str) -> bool:
    return (
        spark.table(SUMMARY_TABLE)
        .where(F.col("observation_signature") == observation_signature)
        .limit(1)
        .count()
        > 0
    )


def collect_counts() -> dict[str, int]:
    active_source_files = (
        spark.table(INGESTION_MANIFEST_TABLE)
        .where(F.col("status") == "active")
        .select("source_file")
        .distinct()
        .count()
    )

    bronze_df = spark.table(BRONZE_TABLE)

    bronze_files = (
        bronze_df
        .select("_source_file")
        .distinct()
        .count()
    )

    bronze_rows = bronze_df.count()
    silver_rows = spark.table(SILVER_TABLE).count()
    quarantine_rows = spark.table(QUARANTINE_TABLE).count()

    return {
        "active_source_files": active_source_files,
        "bronze_files": bronze_files,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
    }


def determine_health(
    *,
    counts: dict[str, int],
    expected_file_count: int,
    minimum_valid_row_pct: float,
    require_exact_row_accounting: bool,
    health_contract: dict,
) -> dict[str, object]:
    bronze_rows = counts["bronze_rows"]
    silver_rows = counts["silver_rows"]
    quarantine_rows = counts["quarantine_rows"]

    if bronze_rows <= 0:
        raise RuntimeError("Bronze is unexpectedly empty.")

    accounted_rows = silver_rows + quarantine_rows

    row_accounting_ok = accounted_rows == bronze_rows

    file_count_ok = (
        counts["active_source_files"] == expected_file_count
        and counts["bronze_files"] == expected_file_count
    )

    valid_pct = silver_rows * 100.0 / bronze_rows

    if (
        (require_exact_row_accounting and not row_accounting_ok)
        or not file_count_ok
    ):
        health_status = health_contract["critical_status"]
    elif valid_pct < minimum_valid_row_pct:
        health_status = health_contract["degraded_status"]
    else:
        health_status = health_contract["healthy_status"]

    return {
        "accounted_rows": accounted_rows,
        "valid_pct": round(valid_pct, 6),
        "row_accounting_ok": row_accounting_ok,
        "file_count_ok": file_count_ok,
        "health_status": health_status,
    }


def build_rule_metrics(
    *,
    observation_signature: str,
    source_input_signature: str,
    bronze_rows: int,
    data_contract: dict,
):
    rule_rows = [
        (
            code,
            str(spec["name"]),
        )
        for code, spec in sorted(data_contract["quality_rules"].items())
    ]

    rules_df = spark.createDataFrame(
        rule_rows,
        "rule_code STRING, rule_name STRING",
    )

    quarantine_counts = (
        spark.table(QUARANTINE_TABLE)
        .select(F.explode("_quality_errors").alias("quality_error"))
        .withColumn(
            "rule_code",
            F.regexp_extract("quality_error", r"^(Q\d{3})", 1),
        )
        .groupBy("rule_code")
        .agg(F.count(F.lit(1)).alias("failed_rows"))
    )

    return (
        rules_df
        .join(quarantine_counts, "rule_code", "left")
        .fillna({"failed_rows": 0})
        .withColumn("failed_rows", F.col("failed_rows").cast("long"))
        .withColumn(
            "failed_pct",
            F.round(
                F.col("failed_rows") * F.lit(100.0) / F.lit(bronze_rows),
                6,
            ),
        )
        .withColumn(
            "observation_signature",
            F.lit(observation_signature),
        )
        .withColumn(
            "source_input_signature",
            F.lit(source_input_signature),
        )
        .withColumn("observed_run_id", F.lit(RUN_ID))
        .withColumn("observed_at_utc", F.current_timestamp())
        .select(
            "observation_signature",
            "source_input_signature",
            "observed_run_id",
            "observed_at_utc",
            "rule_code",
            "rule_name",
            "failed_rows",
            "failed_pct",
        )
    )


def persist_summary(
    *,
    observation_signature: str,
    source_input_signature: str,
    observability_contract_sha256: str,
    observability_contract_version: str,
    counts: dict[str, int],
    health: dict[str, object],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {SUMMARY_TABLE} AS target
        USING (
            SELECT
                '{sql_string(observation_signature)}'
                    AS observation_signature,
                '{sql_string(source_input_signature)}'
                    AS source_input_signature,
                '{sql_string(observability_contract_sha256)}'
                    AS observability_contract_sha256,
                '{sql_string(observability_contract_version)}'
                    AS observability_contract_version,
                '{sql_string(RUN_ID)}' AS observed_run_id,
                current_timestamp() AS observed_at_utc,
                CAST({counts["active_source_files"]} AS BIGINT)
                    AS active_source_files,
                CAST({counts["bronze_files"]} AS BIGINT)
                    AS bronze_files,
                CAST({counts["bronze_rows"]} AS BIGINT)
                    AS bronze_rows,
                CAST({counts["silver_rows"]} AS BIGINT)
                    AS silver_rows,
                CAST({counts["quarantine_rows"]} AS BIGINT)
                    AS quarantine_rows,
                CAST({health["accounted_rows"]} AS BIGINT)
                    AS accounted_rows,
                CAST({health["valid_pct"]} AS DOUBLE)
                    AS valid_pct,
                CAST({str(health["row_accounting_ok"]).lower()} AS BOOLEAN)
                    AS row_accounting_ok,
                CAST({str(health["file_count_ok"]).lower()} AS BOOLEAN)
                    AS file_count_ok,
                '{sql_string(str(health["health_status"]))}'
                    AS health_status
        ) AS source
        ON target.observation_signature = source.observation_signature
        WHEN MATCHED THEN UPDATE SET
            target.source_input_signature =
                source.source_input_signature,
            target.observability_contract_sha256 =
                source.observability_contract_sha256,
            target.observability_contract_version =
                source.observability_contract_version,
            target.observed_run_id = source.observed_run_id,
            target.observed_at_utc = source.observed_at_utc,
            target.active_source_files = source.active_source_files,
            target.bronze_files = source.bronze_files,
            target.bronze_rows = source.bronze_rows,
            target.silver_rows = source.silver_rows,
            target.quarantine_rows = source.quarantine_rows,
            target.accounted_rows = source.accounted_rows,
            target.valid_pct = source.valid_pct,
            target.row_accounting_ok = source.row_accounting_ok,
            target.file_count_ok = source.file_count_ok,
            target.health_status = source.health_status
        WHEN NOT MATCHED THEN INSERT (
            observation_signature,
            source_input_signature,
            observability_contract_sha256,
            observability_contract_version,
            observed_run_id,
            observed_at_utc,
            active_source_files,
            bronze_files,
            bronze_rows,
            silver_rows,
            quarantine_rows,
            accounted_rows,
            valid_pct,
            row_accounting_ok,
            file_count_ok,
            health_status
        )
        VALUES (
            source.observation_signature,
            source.source_input_signature,
            source.observability_contract_sha256,
            source.observability_contract_version,
            source.observed_run_id,
            source.observed_at_utc,
            source.active_source_files,
            source.bronze_files,
            source.bronze_rows,
            source.silver_rows,
            source.quarantine_rows,
            source.accounted_rows,
            source.valid_pct,
            source.row_accounting_ok,
            source.file_count_ok,
            source.health_status
        )
        """
    )


def persist_rule_metrics(rule_metrics_df) -> None:
    observation_signature = (
        rule_metrics_df
        .select("observation_signature")
        .first()["observation_signature"]
    )

    spark.sql(
        f"""
        DELETE FROM {DQ_TABLE}
        WHERE observation_signature =
            '{sql_string(observation_signature)}'
        """
    )

    rule_metrics_df.write.mode("append").saveAsTable(DQ_TABLE)


def create_latest_views() -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {LATEST_HEALTH_VIEW}
        COMMENT 'Most recent pipeline health snapshot'
        AS
        SELECT *
        FROM {SUMMARY_TABLE}
        QUALIFY ROW_NUMBER() OVER (
            ORDER BY observed_at_utc DESC
        ) = 1
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {LATEST_DQ_VIEW}
        COMMENT 'Rule-level metrics for the most recent health snapshot'
        AS
        SELECT dq.*
        FROM {DQ_TABLE} dq
        INNER JOIN {LATEST_HEALTH_VIEW} health
            ON dq.observation_signature =
               health.observation_signature
        """
    )


# COMMAND ----------

log_event("observability_run_started", catalog=CATALOG)

data_contract, data_contract_sha256 = load_yaml_with_hash(
    DATA_CONTRACT_PATH
)
observability_contract, observability_contract_sha256 = (
    load_yaml_with_hash(OBSERVABILITY_CONTRACT_PATH)
)

create_tables()

silver_state = read_silver_state()
source_input_signature = str(silver_state["input_signature"])

observation_signature = compute_observation_signature(
    source_input_signature,
    observability_contract_sha256,
)

if already_observed(observation_signature):
    latest = (
        spark.table(SUMMARY_TABLE)
        .where(
            F.col("observation_signature")
            == observation_signature
        )
        .first()
        .asDict(recursive=True)
    )

    result = {
        "run_id": RUN_ID,
        "status": "SKIPPED",
        "reason": "unchanged_silver_input_and_observability_contract",
        "observation_signature": observation_signature,
        "health_status": latest["health_status"],
        "bronze_rows": latest["bronze_rows"],
        "silver_rows": latest["silver_rows"],
        "quarantine_rows": latest["quarantine_rows"],
        "valid_pct": latest["valid_pct"],
    }

    log_event("observability_run_skipped", **result)
    dbutils.notebook.exit(json.dumps(result))

counts = collect_counts()

quality_contract = observability_contract["quality"]

health = determine_health(
    counts=counts,
    expected_file_count=int(
        quality_contract["expected_file_count"]
    ),
    minimum_valid_row_pct=float(
        quality_contract["minimum_valid_row_pct"]
    ),
    require_exact_row_accounting=bool(
        quality_contract["require_exact_row_accounting"]
    ),
    health_contract=observability_contract["health"],
)

rule_metrics_df = build_rule_metrics(
    observation_signature=observation_signature,
    source_input_signature=source_input_signature,
    bronze_rows=counts["bronze_rows"],
    data_contract=data_contract,
)

persist_summary(
    observation_signature=observation_signature,
    source_input_signature=source_input_signature,
    observability_contract_sha256=observability_contract_sha256,
    observability_contract_version=str(
        observability_contract["contract_version"]
    ),
    counts=counts,
    health=health,
)

persist_rule_metrics(rule_metrics_df)
create_latest_views()

rule_metrics = [
    row.asDict(recursive=True)
    for row in (
        spark.table(DQ_TABLE)
        .where(
            F.col("observation_signature")
            == observation_signature
        )
        .select(
            "rule_code",
            "rule_name",
            "failed_rows",
            "failed_pct",
        )
        .orderBy("rule_code")
        .collect()
    )
]

result = {
    "run_id": RUN_ID,
    "status": "PROCESSED",
    "health_status": health["health_status"],
    "active_source_files": counts["active_source_files"],
    "bronze_files": counts["bronze_files"],
    "bronze_rows": counts["bronze_rows"],
    "silver_rows": counts["silver_rows"],
    "quarantine_rows": counts["quarantine_rows"],
    "accounted_rows": health["accounted_rows"],
    "valid_pct": health["valid_pct"],
    "row_accounting_ok": health["row_accounting_ok"],
    "file_count_ok": health["file_count_ok"],
    "observation_signature": observation_signature,
    "source_input_signature": source_input_signature,
    "observability_contract_version": str(
        observability_contract["contract_version"]
    ),
    "data_contract_sha256": data_contract_sha256,
    "rule_metrics": rule_metrics,
}

log_event("observability_run_completed", **result)

print(json.dumps(result, indent=2, ensure_ascii=False))
print("Evidence level: ENVIRONMENT VALIDATED")

if health["health_status"] == observability_contract["health"]["critical_status"]:
    raise RuntimeError(
        "Critical pipeline-health condition detected after metrics publication."
    )

dbutils.notebook.exit(json.dumps(result))
