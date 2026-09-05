# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transformation and Quarantine
# MAGIC
# MAGIC Normalizes aliases, applies typing and data-quality rules, validates
# MAGIC candidates before publication, preserves the last known good Silver table,
# MAGIC and skips work when Bronze plus the contract are unchanged.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "customer_lakehouse")
dbutils.widgets.text("expected_file_count", "50")

CATALOG = dbutils.widgets.get("catalog").strip()
EXPECTED_FILE_COUNT = int(dbutils.widgets.get("expected_file_count"))

BRONZE_TABLE = f"{CATALOG}.bronze.sales_raw"
INGESTION_MANIFEST_TABLE = f"{CATALOG}.observability.ingestion_manifest"
STATE_TABLE = f"{CATALOG}.observability.transformation_state"
SILVER_TABLE = f"{CATALOG}.silver.sales_transactions"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.sales_transactions_invalid"

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
LAYER_NAME = "silver_sales_transactions"

def log_event(event: str, **details: object) -> None:
    print(json.dumps(
        {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
         "run_id": RUN_ID, "event": event, **details},
        ensure_ascii=False, default=str
    ))

def sql_string(value: str) -> str:
    return value.replace("'", "''")

def resolve_project_root() -> Path:
    context_path = (
        dbutils.notebook.entry_point.getDbutils().notebook()
        .getContext().notebookPath().get()
    )
    physical = Path(context_path) if context_path.startswith("/Workspace/") else Path("/Workspace") / context_path.lstrip("/")
    return physical.parent.parent

PROJECT_ROOT = resolve_project_root()
CONTRACT_PATH = PROJECT_ROOT / "config" / "data_contract.yaml"

def load_contract() -> tuple[dict, str]:
    if not CONTRACT_PATH.exists():
        raise RuntimeError(f"Data contract not found: {CONTRACT_PATH}")
    raw = CONTRACT_PATH.read_bytes()
    contract = yaml.safe_load(raw.decode("utf-8"))
    required = {"contract_version", "dataset", "quality", "columns", "quality_rules"}
    missing = required - set(contract)
    if missing:
        raise RuntimeError("Contract missing sections: " + ", ".join(sorted(missing)))
    return contract, hashlib.sha256(raw).hexdigest()

def ensure_state_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            layer_name STRING,
            input_signature STRING,
            source_snapshot_sha256 STRING,
            contract_sha256 STRING,
            contract_version STRING,
            last_successful_run_id STRING,
            last_successful_at_utc TIMESTAMP,
            source_rows BIGINT,
            output_rows BIGINT,
            quarantine_rows BIGINT
        )
        USING DELTA
        COMMENT 'Successful transformation state used for deterministic skip semantics'
    """)

def current_source_snapshot() -> tuple[str, list[dict[str, str]]]:
    rows = (
        spark.table(INGESTION_MANIFEST_TABLE)
        .where(F.col("status") == "active")
        .select("source_file", "sha256")
        .orderBy("source_file")
        .collect()
    )
    if len(rows) != EXPECTED_FILE_COUNT:
        raise RuntimeError(f"Expected {EXPECTED_FILE_COUNT} active files, found {len(rows)}.")
    values = [{"source_file": str(r["source_file"]), "sha256": str(r["sha256"]).lower()} for r in rows]
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), values

def compute_input_signature(source_snapshot_sha256: str, contract_sha256: str) -> str:
    return hashlib.sha256(f"{source_snapshot_sha256}:{contract_sha256}".encode()).hexdigest()

def target_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)

def previous_state() -> dict[str, object] | None:
    rows = spark.table(STATE_TABLE).where(F.col("layer_name") == LAYER_NAME).limit(1).collect()
    return rows[0].asDict(recursive=True) if rows else None

def outputs_are_consistent(source_rows: int) -> bool:
    if not target_exists(SILVER_TABLE) or not target_exists(QUARANTINE_TABLE):
        return False
    return spark.table(SILVER_TABLE).count() + spark.table(QUARANTINE_TABLE).count() == source_rows

def validate_file_level_contract(bronze_df: DataFrame, contract: dict) -> None:
    variants = bronze_df.select(
        "_source_file", F.sort_array(F.map_keys("payload")).alias("observed_columns")
    ).distinct()

    ambiguous = variants.groupBy("_source_file").count().where(F.col("count") != 1).collect()
    if ambiguous:
        raise RuntimeError("Multiple payload schemas found inside a single source file.")

    schemas = {r["_source_file"]: set(r["observed_columns"]) for r in variants.collect()}
    if len(schemas) != EXPECTED_FILE_COUNT:
        raise RuntimeError(f"Expected schemas for {EXPECTED_FILE_COUNT} files, found {len(schemas)}.")

    violations = []
    for source_file, observed in sorted(schemas.items()):
        for canonical, spec in contract["columns"].items():
            if not spec.get("required", False):
                continue
            accepted = {canonical, *spec.get("aliases", [])}
            if observed.isdisjoint(accepted):
                violations.append(
                    f"{source_file}: missing '{canonical}' (accepted: {sorted(accepted)})"
                )
    if violations:
        raise RuntimeError("Breaking source-schema contract violation:\n" + "\n".join(violations))

def clean_string(column: F.Column) -> F.Column:
    value = F.trim(column.cast("string"))
    return F.when(value == "", F.lit(None)).otherwise(value)

def payload_value(canonical: str, spec: dict) -> F.Column:
    names = [canonical, *spec.get("aliases", [])]
    values = [clean_string(F.element_at("payload", F.lit(name))) for name in names]
    return values[0] if len(values) == 1 else F.coalesce(*values)

def alias_conflict(canonical: str, spec: dict) -> F.Column:
    names = [canonical, *spec.get("aliases", [])]
    if len(names) <= 1:
        return F.lit(False)
    values = F.array(*[
        clean_string(F.element_at("payload", F.lit(name))) for name in names
    ])
    non_null = F.filter(values, lambda x: x.isNotNull())
    return F.size(F.array_distinct(non_null)) > 1

def normalize_decimal_string(column: F.Column) -> F.Column:
    value = F.regexp_replace(column, r"(?i)R\$\s*", "")
    value = F.regexp_replace(value, r"\s+", "")
    return F.when(
        F.instr(value, ",") > 0,
        F.regexp_replace(F.regexp_replace(value, r"\.", ""), ",", "."),
    ).otherwise(value)

def build_candidate(bronze_df: DataFrame, contract: dict) -> DataFrame:
    df = bronze_df
    for canonical, spec in contract["columns"].items():
        df = df.withColumn(f"_raw_{canonical}", payload_value(canonical, spec))

    conflicts = [
        alias_conflict(canonical, spec)
        for canonical, spec in contract["columns"].items()
        if spec.get("aliases")
    ]
    df = df.withColumn(
        "_has_alias_conflict",
        F.aggregate(F.array(*conflicts), F.lit(False), lambda acc, x: acc | x)
        if conflicts else F.lit(False),
    )

    df = (
        df
        .withColumn("_norm_preco_unitario", normalize_decimal_string(F.col("_raw_preco_unitario")))
        .withColumn("_norm_desconto_pct", normalize_decimal_string(
            F.regexp_replace(F.col("_raw_desconto_pct"), "%", "")
        ))
        .withColumn("_discount_has_percent_sign", F.instr(F.col("_raw_desconto_pct"), "%") > 0)
        .withColumn(
            "data_pedido",
            F.expr("""
                coalesce(
                    try_cast(_raw_data_pedido AS DATE),
                    to_date(try_to_timestamp(_raw_data_pedido, 'dd/MM/yyyy'))
                )
            """),
        )
        .withColumn("quantidade", F.expr("try_cast(_raw_quantidade AS INT)"))
        .withColumn("preco_unitario", F.expr(
            "try_cast(_norm_preco_unitario AS DECIMAL(18,2))"
        ))
        .withColumn("_discount_numeric", F.expr(
            "try_cast(_norm_desconto_pct AS DECIMAL(18,6))"
        ))
    )

    df = df.withColumn(
        "desconto_pct",
        F.when(F.col("_raw_desconto_pct").isNull(), F.lit(None).cast("decimal(9,4)"))
        .when(F.col("_discount_has_percent_sign"),
              F.col("_discount_numeric").cast("decimal(9,4)"))
        .when((F.col("_discount_numeric") >= 0) & (F.col("_discount_numeric") <= 1),
              (F.col("_discount_numeric") * 100).cast("decimal(9,4)"))
        .otherwise(F.col("_discount_numeric").cast("decimal(9,4)")),
    )

    string_columns = [
        "id_transacao", "id_cliente_final", "nome_cliente", "email", "sku",
        "produto", "categoria", "forma_pagamento", "status_pedido",
        "canal_venda", "cidade", "uf", "vendedor", "cupom",
    ]
    for name in string_columns:
        df = df.withColumn(name, F.col(f"_raw_{name}"))

    df = df.withColumn(
        "_transaction_occurrences",
        F.when(
            F.col("id_transacao").isNotNull(),
            F.count(F.lit(1)).over(Window.partitionBy("id_transacao")),
        ).otherwise(F.lit(0)),
    )

    missing_business_value = (
        F.col("nome_cliente").isNull()
        | F.col("sku").isNull()
        | F.col("produto").isNull()
        | F.col("categoria").isNull()
        | F.col("forma_pagamento").isNull()
        | F.col("status_pedido").isNull()
    )

    invalid_discount = (
        F.col("_raw_desconto_pct").isNotNull()
        & (
            F.col("desconto_pct").isNull()
            | (F.col("desconto_pct") < 0)
            | (F.col("desconto_pct") > 100)
        )
    )

    errors = F.array(
        F.when(F.col("id_transacao").isNull(), F.lit("Q001_MISSING_TRANSACTION_ID")),
        F.when(F.col("id_cliente_final").isNull(), F.lit("Q002_MISSING_CUSTOMER_ID")),
        F.when(F.col("data_pedido").isNull(), F.lit("Q003_INVALID_ORDER_DATE")),
        F.when(
            F.col("quantidade").isNull() | (F.col("quantidade") <= 0),
            F.lit("Q004_INVALID_QUANTITY"),
        ),
        F.when(
            F.col("preco_unitario").isNull() | (F.col("preco_unitario") < 0),
            F.lit("Q005_INVALID_UNIT_PRICE"),
        ),
        F.when(F.col("_transaction_occurrences") > 1, F.lit("Q006_DUPLICATE_TRANSACTION_ID")),
        F.when(missing_business_value, F.lit("Q007_MISSING_REQUIRED_BUSINESS_VALUE")),
        F.when(invalid_discount, F.lit("Q008_INVALID_DISCOUNT")),
        F.when(F.col("_has_alias_conflict"), F.lit("Q009_ALIAS_CONFLICT")),
    )

    return (
        df
        .withColumn("_quality_errors", F.filter(errors, lambda x: x.isNotNull()))
        .withColumn("_is_valid", F.size("_quality_errors") == 0)
        .withColumn("_bronze_ingested_at_utc", F.col("_ingested_at_utc"))
        .withColumn("_silver_processed_at_utc", F.current_timestamp())
        .withColumn("_silver_run_id", F.lit(RUN_ID))
        .withColumn("_silver_contract_version", F.lit(str(contract["contract_version"])))
    )

SILVER_COLUMNS = [
    "id_transacao", "id_cliente_final", "data_pedido", "nome_cliente", "email",
    "sku", "produto", "categoria", "quantidade", "preco_unitario",
    "desconto_pct", "forma_pagamento", "status_pedido", "canal_venda",
    "cidade", "uf", "vendedor", "cupom", "_source_file", "_source_path",
    "_file_sha256", "_source_row_number", "_bronze_ingested_at_utc",
    "_silver_processed_at_utc", "_silver_run_id", "_silver_contract_version",
]
QUARANTINE_COLUMNS = [*SILVER_COLUMNS, "payload", "_quality_errors"]

def validate_candidates(
    bronze_rows: int,
    silver_df: DataFrame,
    quarantine_df: DataFrame,
    minimum_valid_pct: float,
) -> dict[str, object]:
    silver_rows = silver_df.count()
    quarantine_rows = quarantine_df.count()

    if silver_rows + quarantine_rows != bronze_rows:
        raise RuntimeError("Candidate accounting violation.")
    if bronze_rows <= 0:
        raise RuntimeError("Bronze snapshot is unexpectedly empty.")

    valid_pct = silver_rows * 100.0 / bronze_rows
    if valid_pct < minimum_valid_pct:
        raise RuntimeError(
            f"Silver quality SLA failed: {valid_pct:.4f}% < {minimum_valid_pct:.4f}%."
        )

    duplicates = (
        silver_df.groupBy("id_transacao").count()
        .where(F.col("count") > 1).limit(1).count()
    )
    if duplicates:
        raise RuntimeError("Candidate Silver contains duplicate id_transacao.")

    if silver_df.columns != SILVER_COLUMNS:
        raise RuntimeError("Candidate Silver schema/order differs from contract.")
    if quarantine_df.columns != QUARANTINE_COLUMNS:
        raise RuntimeError("Candidate Quarantine schema/order differs from contract.")

    errors = {
        row["quality_error"]: row["count"]
        for row in (
            quarantine_df.select(F.explode("_quality_errors").alias("quality_error"))
            .groupBy("quality_error").count().orderBy("quality_error").collect()
        )
    }

    return {
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "valid_pct": round(valid_pct, 6),
        "quality_error_counts": errors,
    }

def publish_candidate(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(table_name)
    )

def publish_targets(silver_candidate: str, quarantine_candidate: str) -> None:
    # Quarantine first; consumer-facing Silver is published last.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {QUARANTINE_TABLE}
        USING DELTA
        COMMENT 'Invalid sales records rejected by the Silver data contract'
        AS SELECT * FROM {quarantine_candidate}
    """)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_TABLE}
        USING DELTA
        COMMENT 'Canonical typed sales transactions validated by the Silver data contract'
        AS SELECT * FROM {silver_candidate}
    """)

def persist_state(
    input_signature: str,
    source_snapshot_sha256: str,
    contract_sha256: str,
    contract_version: str,
    source_rows: int,
    output_rows: int,
    quarantine_rows: int,
) -> None:
    spark.sql(f"""
        MERGE INTO {STATE_TABLE} AS target
        USING (
            SELECT
                '{sql_string(LAYER_NAME)}' AS layer_name,
                '{sql_string(input_signature)}' AS input_signature,
                '{sql_string(source_snapshot_sha256)}' AS source_snapshot_sha256,
                '{sql_string(contract_sha256)}' AS contract_sha256,
                '{sql_string(contract_version)}' AS contract_version,
                '{sql_string(RUN_ID)}' AS run_id,
                current_timestamp() AS event_time,
                CAST({source_rows} AS BIGINT) AS source_rows,
                CAST({output_rows} AS BIGINT) AS output_rows,
                CAST({quarantine_rows} AS BIGINT) AS quarantine_rows
        ) AS source
        ON target.layer_name = source.layer_name
        WHEN MATCHED THEN UPDATE SET
            target.input_signature = source.input_signature,
            target.source_snapshot_sha256 = source.source_snapshot_sha256,
            target.contract_sha256 = source.contract_sha256,
            target.contract_version = source.contract_version,
            target.last_successful_run_id = source.run_id,
            target.last_successful_at_utc = source.event_time,
            target.source_rows = source.source_rows,
            target.output_rows = source.output_rows,
            target.quarantine_rows = source.quarantine_rows
        WHEN NOT MATCHED THEN INSERT *
    """)

log_event("silver_run_started", catalog=CATALOG)

contract, contract_sha256 = load_contract()
ensure_state_table()

source_snapshot_sha256, manifest_files = current_source_snapshot()
input_signature = compute_input_signature(source_snapshot_sha256, contract_sha256)

bronze_df = spark.table(BRONZE_TABLE)
bronze_rows = bronze_df.count()
validate_file_level_contract(bronze_df, contract)

state = previous_state()
if (
    state is not None
    and state["input_signature"] == input_signature
    and outputs_are_consistent(bronze_rows)
):
    result = {
        "run_id": RUN_ID,
        "status": "SKIPPED",
        "reason": "unchanged_bronze_snapshot_and_contract",
        "source_files": len(manifest_files),
        "bronze_rows": bronze_rows,
        "silver_rows": spark.table(SILVER_TABLE).count(),
        "quarantine_rows": spark.table(QUARANTINE_TABLE).count(),
        "contract_version": str(contract["contract_version"]),
        "input_signature": input_signature,
    }
    log_event("silver_run_skipped", **result)
    dbutils.notebook.exit(json.dumps(result))

candidate = build_candidate(bronze_df, contract).cache()
silver_df = candidate.where(F.col("_is_valid")).select(*SILVER_COLUMNS).cache()
quarantine_df = candidate.where(~F.col("_is_valid")).select(*QUARANTINE_COLUMNS).cache()

metrics = validate_candidates(
    bronze_rows,
    silver_df,
    quarantine_df,
    float(contract["quality"]["minimum_valid_row_pct"]),
)

safe_run_id = RUN_ID.replace("-", "_").replace(":", "_")
silver_candidate = f"{CATALOG}.silver._sales_transactions_candidate_{safe_run_id}"
quarantine_candidate = (
    f"{CATALOG}.quarantine._sales_transactions_invalid_candidate_{safe_run_id}"
)

try:
    publish_candidate(silver_df, silver_candidate)
    publish_candidate(quarantine_df, quarantine_candidate)

    if spark.table(silver_candidate).count() != metrics["silver_rows"]:
        raise RuntimeError("Silver candidate read-back count failed.")
    if spark.table(quarantine_candidate).count() != metrics["quarantine_rows"]:
        raise RuntimeError("Quarantine candidate read-back count failed.")

    publish_targets(silver_candidate, quarantine_candidate)

    published_silver_rows = spark.table(SILVER_TABLE).count()
    published_quarantine_rows = spark.table(QUARANTINE_TABLE).count()

    if published_silver_rows != metrics["silver_rows"]:
        raise RuntimeError("Published Silver row count differs from candidate.")
    if published_quarantine_rows != metrics["quarantine_rows"]:
        raise RuntimeError("Published Quarantine row count differs from candidate.")

    persist_state(
        input_signature,
        source_snapshot_sha256,
        contract_sha256,
        str(contract["contract_version"]),
        bronze_rows,
        published_silver_rows,
        published_quarantine_rows,
    )
finally:
    spark.catalog.clearCache()
    spark.sql(f"DROP TABLE IF EXISTS {silver_candidate}")
    spark.sql(f"DROP TABLE IF EXISTS {quarantine_candidate}")

result = {
    "run_id": RUN_ID,
    "status": "PROCESSED",
    "source_files": len(manifest_files),
    "bronze_rows": bronze_rows,
    "silver_rows": metrics["silver_rows"],
    "quarantine_rows": metrics["quarantine_rows"],
    "valid_pct": metrics["valid_pct"],
    "quality_error_counts": metrics["quality_error_counts"],
    "contract_version": str(contract["contract_version"]),
    "contract_sha256": contract_sha256,
    "source_snapshot_sha256": source_snapshot_sha256,
    "input_signature": input_signature,
}

log_event("silver_run_completed", **result)
print(json.dumps(result, indent=2, ensure_ascii=False))
print("Evidence level: ENVIRONMENT VALIDATED")
dbutils.notebook.exit(json.dumps(result))
