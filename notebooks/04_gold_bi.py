# Databricks notebook source
# MAGIC %md
# MAGIC # Gold BI Star Schema
# MAGIC
# MAGIC Builds a Power BI-oriented star schema from the validated Silver layer.
# MAGIC The job gates on current observability health, validates all candidates,
# MAGIC publishes dimensions first and the fact last, and records successful
# MAGIC publication state for deterministic no-change skips.

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

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
CATALOG = dbutils.widgets.get("catalog").strip()

SILVER_TABLE = f"{CATALOG}.silver.sales_transactions"
TRANSFORMATION_STATE_TABLE = f"{CATALOG}.observability.transformation_state"
LATEST_HEALTH_VIEW = f"{CATALOG}.observability.v_pipeline_health_latest"

DIM_DATE_TABLE = f"{CATALOG}.gold_bi.dim_date"
DIM_CUSTOMER_TABLE = f"{CATALOG}.gold_bi.dim_customer"
DIM_PRODUCT_TABLE = f"{CATALOG}.gold_bi.dim_product"
FACT_SALES_TABLE = f"{CATALOG}.gold_bi.fact_sales"

STATE_TABLE = f"{CATALOG}.observability.gold_publication_state"

SILVER_LAYER_NAME = "silver_sales_transactions"
GOLD_LAYER_NAME = "gold_bi_star_schema"

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
GOLD_CONTRACT_PATH = PROJECT_ROOT / "config" / "gold_contract.yaml"

# COMMAND ----------

def load_contract() -> tuple[dict, str]:
    if not GOLD_CONTRACT_PATH.exists():
        raise RuntimeError(f"Gold contract not found: {GOLD_CONTRACT_PATH}")

    raw = GOLD_CONTRACT_PATH.read_bytes()
    contract = yaml.safe_load(raw.decode("utf-8"))

    required_sections = {
        "contract_version",
        "dataset",
        "observability_gate",
        "privacy",
        "model",
        "tables",
        "quality",
        "publication",
    }

    missing = required_sections - set(contract)

    if missing:
        raise RuntimeError(
            "Gold contract is missing required sections: "
            + ", ".join(sorted(missing))
        )

    return contract, hashlib.sha256(raw).hexdigest()


def ensure_state_table() -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            layer_name STRING,
            input_signature STRING,
            silver_input_signature STRING,
            gold_contract_sha256 STRING,
            contract_version STRING,
            last_successful_run_id STRING,
            last_successful_at_utc TIMESTAMP,
            silver_rows BIGINT,
            dim_date_rows BIGINT,
            dim_customer_rows BIGINT,
            dim_product_rows BIGINT,
            fact_sales_rows BIGINT
        )
        USING DELTA
        COMMENT 'Successful Gold BI publication state'
        """
    )


def read_silver_state() -> dict[str, object]:
    rows = (
        spark.table(TRANSFORMATION_STATE_TABLE)
        .where(F.col("layer_name") == SILVER_LAYER_NAME)
        .limit(1)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            "Silver transformation state is missing. "
            "Gold cannot publish without a successful Silver state."
        )

    state = rows[0].asDict(recursive=True)

    if not state.get("input_signature"):
        raise RuntimeError("Silver state has no input_signature.")

    return state


def validate_observability_gate(
    silver_input_signature: str,
    contract: dict,
) -> dict[str, object]:
    if not spark.catalog.tableExists(LATEST_HEALTH_VIEW):
        raise RuntimeError(
            "Latest pipeline health view is missing. "
            "Run the observability phase before Gold."
        )

    row = spark.table(LATEST_HEALTH_VIEW).first()

    if row is None:
        raise RuntimeError("Latest pipeline health view is empty.")

    health = row.asDict(recursive=True)
    gate = contract["observability_gate"]

    required_status = str(gate["required_health_status"])

    if health["health_status"] != required_status:
        raise RuntimeError(
            "Gold publication blocked by observability gate: "
            f"health={health['health_status']}, required={required_status}."
        )

    if bool(gate["require_matching_source_input_signature"]):
        if health["source_input_signature"] != silver_input_signature:
            raise RuntimeError(
                "Gold publication blocked: observability evidence does not "
                "match the current Silver input signature."
            )

    return health


def compute_input_signature(
    silver_input_signature: str,
    gold_contract_sha256: str,
) -> str:
    value = (
        f"{silver_input_signature}:{gold_contract_sha256}"
    ).encode("utf-8")

    return hashlib.sha256(value).hexdigest()


def previous_state() -> dict[str, object] | None:
    rows = (
        spark.table(STATE_TABLE)
        .where(F.col("layer_name") == GOLD_LAYER_NAME)
        .limit(1)
        .collect()
    )

    return rows[0].asDict(recursive=True) if rows else None


def all_targets_exist() -> bool:
    return all(
        spark.catalog.tableExists(table_name)
        for table_name in [
            DIM_DATE_TABLE,
            DIM_CUSTOMER_TABLE,
            DIM_PRODUCT_TABLE,
            FACT_SALES_TABLE,
        ]
    )


def targets_are_consistent(expected_fact_rows: int) -> bool:
    if not all_targets_exist():
        return False

    if spark.table(FACT_SALES_TABLE).count() != expected_fact_rows:
        return False

    for table_name, key_column in [
        (DIM_DATE_TABLE, "date_key"),
        (DIM_CUSTOMER_TABLE, "customer_key"),
        (DIM_PRODUCT_TABLE, "product_key"),
    ]:
        duplicate = (
            spark.table(table_name)
            .groupBy(key_column)
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate:
            return False

    return True


def surrogate_key(column_name: str) -> F.Column:
    return F.xxhash64(F.col(column_name)).cast("long")


def build_dim_date(silver_df: DataFrame) -> DataFrame:
    bounds = (
        silver_df
        .agg(
            F.min("data_pedido").alias("min_date"),
            F.max("data_pedido").alias("max_date"),
        )
        .first()
    )

    min_date = bounds["min_date"]
    max_date = bounds["max_date"]

    if min_date is None or max_date is None:
        raise RuntimeError("Silver has no valid order-date range.")

    calendar = spark.sql(
        f"""
        SELECT explode(
            sequence(
                DATE '{min_date}',
                DATE '{max_date}',
                INTERVAL 1 DAY
            )
        ) AS full_date
        """
    )

    return (
        calendar
        .select(
            F.date_format("full_date", "yyyyMMdd")
            .cast("int")
            .alias("date_key"),
            F.col("full_date"),
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month_number"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.date_format("full_date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.dayofweek("full_date")
            .isin([1, 7])
            .alias("is_weekend"),
        )
    )


def build_dim_customer(silver_df: DataFrame) -> DataFrame:
    latest_window = (
        Window.partitionBy("id_cliente_final")
        .orderBy(
            F.col("data_pedido").desc(),
            F.col("_source_file").desc(),
            F.col("_source_row_number").desc(),
        )
    )

    return (
        silver_df
        .withColumn("_rank", F.row_number().over(latest_window))
        .where(F.col("_rank") == 1)
        .select(
            surrogate_key("id_cliente_final").alias("customer_key"),
            F.col("id_cliente_final").alias("customer_id"),
            F.col("nome_cliente").alias("customer_name"),
            F.col("cidade").alias("city"),
            F.col("uf").alias("state"),
            F.col("data_pedido").alias("last_order_date"),
        )
    )


def build_dim_product(silver_df: DataFrame) -> DataFrame:
    latest_window = (
        Window.partitionBy("sku")
        .orderBy(
            F.col("data_pedido").desc(),
            F.col("_source_file").desc(),
            F.col("_source_row_number").desc(),
        )
    )

    return (
        silver_df
        .withColumn("_rank", F.row_number().over(latest_window))
        .where(F.col("_rank") == 1)
        .select(
            surrogate_key("sku").alias("product_key"),
            F.col("sku"),
            F.col("produto").alias("product_name"),
            F.col("categoria").alias("category"),
            F.col("data_pedido").alias("last_order_date"),
        )
    )


def build_fact_sales(silver_df: DataFrame) -> DataFrame:
    gross_amount = (
        F.col("quantidade").cast("decimal(18,4)")
        * F.col("preco_unitario").cast("decimal(18,4)")
    )

    normalized_discount_pct = F.coalesce(
        F.col("desconto_pct").cast("decimal(18,6)"),
        F.lit(0).cast("decimal(18,6)"),
    )

    discount_amount = (
        gross_amount
        * normalized_discount_pct
        / F.lit(100)
    )

    net_amount = gross_amount - discount_amount

    return (
        silver_df
        .select(
            F.col("id_transacao").alias("transaction_id"),
            F.date_format("data_pedido", "yyyyMMdd")
            .cast("int")
            .alias("date_key"),
            surrogate_key("id_cliente_final").alias("customer_key"),
            surrogate_key("sku").alias("product_key"),
            F.col("quantidade").alias("quantity"),
            F.col("preco_unitario").alias("unit_price"),
            normalized_discount_pct
            .cast("decimal(9,4)")
            .alias("discount_pct"),
            gross_amount
            .cast("decimal(20,4)")
            .alias("gross_amount"),
            discount_amount
            .cast("decimal(20,4)")
            .alias("discount_amount"),
            net_amount
            .cast("decimal(20,4)")
            .alias("net_amount"),
            F.col("forma_pagamento").alias("payment_method"),
            F.col("status_pedido").alias("order_status"),
            F.col("canal_venda").alias("sales_channel"),
            F.col("vendedor").alias("seller"),
            F.col("cupom").alias("coupon"),
        )
    )


def expected_columns(contract: dict, table_name: str) -> list[str]:
    return list(contract["tables"][table_name]["columns"])


def validate_exact_columns(
    dataframe: DataFrame,
    table_name: str,
    contract: dict,
) -> None:
    expected = expected_columns(contract, table_name)

    if dataframe.columns != expected:
        raise RuntimeError(
            f"{table_name} columns differ from Gold contract. "
            f"Expected={expected}, actual={dataframe.columns}"
        )


def validate_unique_key(
    dataframe: DataFrame,
    table_name: str,
    key_column: str,
) -> None:
    invalid_null = (
        dataframe
        .where(F.col(key_column).isNull())
        .limit(1)
        .count()
    )

    if invalid_null:
        raise RuntimeError(
            f"{table_name}.{key_column} contains NULL."
        )

    duplicate = (
        dataframe
        .groupBy(key_column)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate:
        raise RuntimeError(
            f"{table_name}.{key_column} is not unique."
        )


def validate_referential_integrity(
    fact_df: DataFrame,
    dim_date_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
) -> None:
    checks = [
        ("date_key", dim_date_df.select("date_key")),
        ("customer_key", dim_customer_df.select("customer_key")),
        ("product_key", dim_product_df.select("product_key")),
    ]

    for key_column, dimension_keys in checks:
        missing = (
            fact_df
            .select(key_column)
            .distinct()
            .join(
                dimension_keys,
                key_column,
                "left_anti",
            )
            .limit(1)
            .count()
        )

        if missing:
            raise RuntimeError(
                f"fact_sales has orphan {key_column} values."
            )


def validate_candidates(
    *,
    silver_rows: int,
    dim_date_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
    fact_df: DataFrame,
    contract: dict,
) -> dict[str, int]:
    frames = {
        "dim_date": dim_date_df,
        "dim_customer": dim_customer_df,
        "dim_product": dim_product_df,
        "fact_sales": fact_df,
    }

    for table_name, dataframe in frames.items():
        validate_exact_columns(
            dataframe,
            table_name,
            contract,
        )

    validate_unique_key(dim_date_df, "dim_date", "date_key")
    validate_unique_key(
        dim_customer_df,
        "dim_customer",
        "customer_key",
    )
    validate_unique_key(
        dim_product_df,
        "dim_product",
        "product_key",
    )
    validate_unique_key(
        fact_df,
        "fact_sales",
        "transaction_id",
    )

    fact_rows = fact_df.count()

    if fact_rows != silver_rows:
        raise RuntimeError(
            "Gold fact row count differs from Silver: "
            f"Silver={silver_rows}, fact_sales={fact_rows}."
        )

    validate_referential_integrity(
        fact_df,
        dim_date_df,
        dim_customer_df,
        dim_product_df,
    )

    negative_amount = (
        fact_df
        .where(
            (F.col("gross_amount") < 0)
            | (F.col("discount_amount") < 0)
            | (F.col("net_amount") < 0)
        )
        .limit(1)
        .count()
    )

    if negative_amount:
        raise RuntimeError(
            "Gold fact contains negative monetary amounts."
        )

    # Explicit collision checks between business and surrogate keys.
    customer_collision = (
        dim_customer_df
        .groupBy("customer_key")
        .agg(
            F.countDistinct("customer_id")
            .alias("business_keys")
        )
        .where(F.col("business_keys") > 1)
        .limit(1)
        .count()
    )

    product_collision = (
        dim_product_df
        .groupBy("product_key")
        .agg(
            F.countDistinct("sku")
            .alias("business_keys")
        )
        .where(F.col("business_keys") > 1)
        .limit(1)
        .count()
    )

    if customer_collision or product_collision:
        raise RuntimeError(
            "Hash surrogate-key collision detected."
        )

    return {
        "dim_date_rows": dim_date_df.count(),
        "dim_customer_rows": dim_customer_df.count(),
        "dim_product_rows": dim_product_df.count(),
        "fact_sales_rows": fact_rows,
    }


def write_candidate(
    dataframe: DataFrame,
    table_name: str,
) -> None:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def publish_table(
    candidate_table: str,
    target_table: str,
    comment: str,
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {target_table}
        USING DELTA
        COMMENT '{sql_string(comment)}'
        AS SELECT * FROM {candidate_table}
        """
    )


def persist_state(
    *,
    input_signature: str,
    silver_input_signature: str,
    gold_contract_sha256: str,
    contract_version: str,
    silver_rows: int,
    counts: dict[str, int],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {STATE_TABLE} AS target
        USING (
            SELECT
                '{sql_string(GOLD_LAYER_NAME)}'
                    AS layer_name,
                '{sql_string(input_signature)}'
                    AS input_signature,
                '{sql_string(silver_input_signature)}'
                    AS silver_input_signature,
                '{sql_string(gold_contract_sha256)}'
                    AS gold_contract_sha256,
                '{sql_string(contract_version)}'
                    AS contract_version,
                '{sql_string(RUN_ID)}'
                    AS last_successful_run_id,
                current_timestamp()
                    AS last_successful_at_utc,
                CAST({silver_rows} AS BIGINT)
                    AS silver_rows,
                CAST({counts["dim_date_rows"]} AS BIGINT)
                    AS dim_date_rows,
                CAST({counts["dim_customer_rows"]} AS BIGINT)
                    AS dim_customer_rows,
                CAST({counts["dim_product_rows"]} AS BIGINT)
                    AS dim_product_rows,
                CAST({counts["fact_sales_rows"]} AS BIGINT)
                    AS fact_sales_rows
        ) AS source
        ON target.layer_name = source.layer_name
        WHEN MATCHED THEN UPDATE SET
            target.input_signature =
                source.input_signature,
            target.silver_input_signature =
                source.silver_input_signature,
            target.gold_contract_sha256 =
                source.gold_contract_sha256,
            target.contract_version =
                source.contract_version,
            target.last_successful_run_id =
                source.last_successful_run_id,
            target.last_successful_at_utc =
                source.last_successful_at_utc,
            target.silver_rows =
                source.silver_rows,
            target.dim_date_rows =
                source.dim_date_rows,
            target.dim_customer_rows =
                source.dim_customer_rows,
            target.dim_product_rows =
                source.dim_product_rows,
            target.fact_sales_rows =
                source.fact_sales_rows
        WHEN NOT MATCHED THEN INSERT (
            layer_name,
            input_signature,
            silver_input_signature,
            gold_contract_sha256,
            contract_version,
            last_successful_run_id,
            last_successful_at_utc,
            silver_rows,
            dim_date_rows,
            dim_customer_rows,
            dim_product_rows,
            fact_sales_rows
        )
        VALUES (
            source.layer_name,
            source.input_signature,
            source.silver_input_signature,
            source.gold_contract_sha256,
            source.contract_version,
            source.last_successful_run_id,
            source.last_successful_at_utc,
            source.silver_rows,
            source.dim_date_rows,
            source.dim_customer_rows,
            source.dim_product_rows,
            source.fact_sales_rows
        )
        """
    )


# COMMAND ----------

log_event("gold_bi_run_started", catalog=CATALOG)

contract, gold_contract_sha256 = load_contract()
ensure_state_table()

silver_state = read_silver_state()
silver_input_signature = str(silver_state["input_signature"])

health = validate_observability_gate(
    silver_input_signature,
    contract,
)

input_signature = compute_input_signature(
    silver_input_signature,
    gold_contract_sha256,
)

silver_df = spark.table(SILVER_TABLE)
silver_rows = silver_df.count()

state = previous_state()

if (
    state is not None
    and state["input_signature"] == input_signature
    and targets_are_consistent(silver_rows)
):
    result = {
        "run_id": RUN_ID,
        "status": "SKIPPED",
        "reason": "unchanged_silver_input_and_gold_contract",
        "health_status": health["health_status"],
        "silver_rows": silver_rows,
        "dim_date_rows": spark.table(DIM_DATE_TABLE).count(),
        "dim_customer_rows": spark.table(DIM_CUSTOMER_TABLE).count(),
        "dim_product_rows": spark.table(DIM_PRODUCT_TABLE).count(),
        "fact_sales_rows": spark.table(FACT_SALES_TABLE).count(),
        "contract_version": str(contract["contract_version"]),
        "input_signature": input_signature,
    }

    log_event("gold_bi_run_skipped", **result)
    dbutils.notebook.exit(json.dumps(result))

dim_date_df = build_dim_date(silver_df)
dim_customer_df = build_dim_customer(silver_df)
dim_product_df = build_dim_product(silver_df)
fact_sales_df = build_fact_sales(silver_df)

counts = validate_candidates(
    silver_rows=silver_rows,
    dim_date_df=dim_date_df,
    dim_customer_df=dim_customer_df,
    dim_product_df=dim_product_df,
    fact_df=fact_sales_df,
    contract=contract,
)

safe_run_id = RUN_ID.replace("-", "_").replace(":", "_")

candidates = {
    "dim_date": (
        dim_date_df,
        f"{CATALOG}.gold_bi._dim_date_candidate_{safe_run_id}",
    ),
    "dim_customer": (
        dim_customer_df,
        f"{CATALOG}.gold_bi._dim_customer_candidate_{safe_run_id}",
    ),
    "dim_product": (
        dim_product_df,
        f"{CATALOG}.gold_bi._dim_product_candidate_{safe_run_id}",
    ),
    "fact_sales": (
        fact_sales_df,
        f"{CATALOG}.gold_bi._fact_sales_candidate_{safe_run_id}",
    ),
}

try:
    for dataframe, candidate_table in candidates.values():
        write_candidate(dataframe, candidate_table)

    # Read-back validation prevents publication of truncated candidates.
    expected_candidate_counts = {
        "dim_date": counts["dim_date_rows"],
        "dim_customer": counts["dim_customer_rows"],
        "dim_product": counts["dim_product_rows"],
        "fact_sales": counts["fact_sales_rows"],
    }

    for name, (_, candidate_table) in candidates.items():
        actual = spark.table(candidate_table).count()

        if actual != expected_candidate_counts[name]:
            raise RuntimeError(
                f"{name} candidate read-back count failed: "
                f"expected={expected_candidate_counts[name]}, actual={actual}"
            )

    # Publish dimensions first and consumer-facing fact last.
    publish_table(
        candidates["dim_date"][1],
        DIM_DATE_TABLE,
        "Calendar dimension for Gold BI",
    )
    publish_table(
        candidates["dim_customer"][1],
        DIM_CUSTOMER_TABLE,
        "Customer dimension; direct email is intentionally excluded",
    )
    publish_table(
        candidates["dim_product"][1],
        DIM_PRODUCT_TABLE,
        "Product dimension for Gold BI",
    )
    publish_table(
        candidates["fact_sales"][1],
        FACT_SALES_TABLE,
        "Transaction-grain sales fact for Power BI consumption",
    )

    # Final publication gate.
    published_counts = {
        "dim_date_rows": spark.table(DIM_DATE_TABLE).count(),
        "dim_customer_rows": spark.table(DIM_CUSTOMER_TABLE).count(),
        "dim_product_rows": spark.table(DIM_PRODUCT_TABLE).count(),
        "fact_sales_rows": spark.table(FACT_SALES_TABLE).count(),
    }

    if published_counts != counts:
        raise RuntimeError(
            "Published Gold row counts differ from validated candidates."
        )

    # Check privacy policy after publication too.
    forbidden_columns = {
        str(value)
        for value in contract["privacy"]["forbidden_columns"]
    }

    for table_name in [
        DIM_DATE_TABLE,
        DIM_CUSTOMER_TABLE,
        DIM_PRODUCT_TABLE,
        FACT_SALES_TABLE,
    ]:
        exposed = forbidden_columns.intersection(
            spark.table(table_name).columns
        )

        if exposed:
            raise RuntimeError(
                f"Forbidden Gold BI columns exposed by {table_name}: "
                + ", ".join(sorted(exposed))
            )

    persist_state(
        input_signature=input_signature,
        silver_input_signature=silver_input_signature,
        gold_contract_sha256=gold_contract_sha256,
        contract_version=str(contract["contract_version"]),
        silver_rows=silver_rows,
        counts=counts,
    )

finally:
    for _, candidate_table in candidates.values():
        spark.sql(f"DROP TABLE IF EXISTS {candidate_table}")


result = {
    "run_id": RUN_ID,
    "status": "PROCESSED",
    "health_status": health["health_status"],
    "silver_rows": silver_rows,
    **counts,
    "contract_version": str(contract["contract_version"]),
    "gold_contract_sha256": gold_contract_sha256,
    "silver_input_signature": silver_input_signature,
    "input_signature": input_signature,
}

log_event("gold_bi_run_completed", **result)
print(json.dumps(result, indent=2, ensure_ascii=False))
print("Evidence level: ENVIRONMENT VALIDATED")

dbutils.notebook.exit(json.dumps(result))
