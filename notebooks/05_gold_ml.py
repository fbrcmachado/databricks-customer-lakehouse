# Databricks notebook source
# MAGIC %md
# MAGIC # Gold ML + MLflow
# MAGIC
# MAGIC Builds privacy-aware ML datasets directly from validated Silver,
# MAGIC trains deterministic KMeans customer segmentation, tracks the run in
# MAGIC Databricks MLflow, validates candidates, and publishes Gold ML safely.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import platform
import uuid
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.sklearn
import sklearn
import yaml
from pyspark.sql import DataFrame, functions as F
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
CATALOG = dbutils.widgets.get("catalog").strip()

SILVER_TABLE = f"{CATALOG}.silver.sales_transactions"
TRANSFORMATION_STATE_TABLE = f"{CATALOG}.observability.transformation_state"
LATEST_HEALTH_VIEW = f"{CATALOG}.observability.v_pipeline_health_latest"

CUSTOMER_FEATURES_TABLE = f"{CATALOG}.gold_ml.customer_features"
PRODUCT_METRICS_TABLE = f"{CATALOG}.gold_ml.product_metrics"
CUSTOMER_SEGMENTS_TABLE = f"{CATALOG}.gold_ml.customer_segments"

STATE_TABLE = f"{CATALOG}.observability.ml_publication_state"

SILVER_LAYER_NAME = "silver_sales_transactions"
ML_LAYER_NAME = "gold_ml_customer_segmentation"

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
ML_CONTRACT_PATH = PROJECT_ROOT / "config" / "ml_contract.yaml"

# COMMAND ----------

def load_contract() -> tuple[dict, str]:
    if not ML_CONTRACT_PATH.exists():
        raise RuntimeError(f"ML contract not found: {ML_CONTRACT_PATH}")

    raw = ML_CONTRACT_PATH.read_bytes()
    contract = yaml.safe_load(raw.decode("utf-8"))

    required = {
        "contract_version",
        "source",
        "observability_gate",
        "privacy",
        "datasets",
        "model",
        "mlflow",
        "runtime",
        "quality",
        "publication",
    }

    missing = required - set(contract)

    if missing:
        raise RuntimeError(
            "ML contract is missing sections: "
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
            ml_contract_sha256 STRING,
            contract_version STRING,
            runtime_fingerprint STRING,
            mlflow_run_id STRING,
            model_uri STRING,
            last_successful_run_id STRING,
            last_successful_at_utc TIMESTAMP,
            customer_features_rows BIGINT,
            product_metrics_rows BIGINT,
            customer_segments_rows BIGINT
        )
        USING DELTA
        COMMENT 'Successful Gold ML publication and MLflow linkage state'
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
            "Gold ML requires a successful Silver state."
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
            "Run observability before Gold ML."
        )

    row = spark.table(LATEST_HEALTH_VIEW).first()

    if row is None:
        raise RuntimeError("Latest pipeline health view is empty.")

    health = row.asDict(recursive=True)
    gate = contract["observability_gate"]

    required_status = str(gate["required_health_status"])

    if health["health_status"] != required_status:
        raise RuntimeError(
            "Gold ML blocked by observability gate: "
            f"health={health['health_status']}, required={required_status}."
        )

    if bool(gate["require_matching_source_input_signature"]):
        if health["source_input_signature"] != silver_input_signature:
            raise RuntimeError(
                "Gold ML blocked: observability evidence does not "
                "match the current Silver input signature."
            )

    return health


def runtime_fingerprint() -> str:
    return "|".join(
        [
            f"python={platform.python_version()}",
            f"sklearn={sklearn.__version__}",
            f"mlflow={mlflow.__version__}",
        ]
    )


def compute_input_signature(
    silver_input_signature: str,
    ml_contract_sha256: str,
    runtime_signature: str,
) -> str:
    value = (
        f"{silver_input_signature}:"
        f"{ml_contract_sha256}:"
        f"{runtime_signature}"
    ).encode("utf-8")

    return hashlib.sha256(value).hexdigest()


def previous_state() -> dict[str, object] | None:
    rows = (
        spark.table(STATE_TABLE)
        .where(F.col("layer_name") == ML_LAYER_NAME)
        .limit(1)
        .collect()
    )

    return rows[0].asDict(recursive=True) if rows else None


def all_targets_exist() -> bool:
    return all(
        spark.catalog.tableExists(name)
        for name in [
            CUSTOMER_FEATURES_TABLE,
            PRODUCT_METRICS_TABLE,
            CUSTOMER_SEGMENTS_TABLE,
        ]
    )


def targets_are_consistent(state: dict[str, object]) -> bool:
    if not all_targets_exist():
        return False

    expected = {
        CUSTOMER_FEATURES_TABLE: int(state["customer_features_rows"]),
        PRODUCT_METRICS_TABLE: int(state["product_metrics_rows"]),
        CUSTOMER_SEGMENTS_TABLE: int(state["customer_segments_rows"]),
    }

    for table_name, expected_rows in expected.items():
        if spark.table(table_name).count() != expected_rows:
            return False

    customer_duplicate = (
        spark.table(CUSTOMER_FEATURES_TABLE)
        .groupBy("customer_key")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )

    segment_duplicate = (
        spark.table(CUSTOMER_SEGMENTS_TABLE)
        .groupBy("customer_key")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )

    return customer_duplicate == 0 and segment_duplicate == 0


def customer_key() -> F.Column:
    return F.xxhash64(F.col("id_cliente_final")).cast("long")


def product_key() -> F.Column:
    return F.xxhash64(F.col("sku")).cast("long")


def net_revenue_expression() -> F.Column:
    quantity = F.col("quantidade").cast("double")
    unit_price = F.col("preco_unitario").cast("double")
    discount_pct = F.coalesce(
        F.col("desconto_pct").cast("double"),
        F.lit(0.0),
    )

    return quantity * unit_price * (
        F.lit(1.0) - discount_pct / F.lit(100.0)
    )


def build_customer_features(silver_df: DataFrame) -> DataFrame:
    as_of_date = (
        silver_df
        .agg(F.max("data_pedido").alias("as_of_date"))
        .first()["as_of_date"]
    )

    if as_of_date is None:
        raise RuntimeError("Cannot build ML features without an as-of date.")

    base = silver_df.withColumn("_net_revenue", net_revenue_expression())

    return (
        base
        .groupBy("id_cliente_final")
        .agg(
            F.countDistinct("id_transacao")
            .cast("long")
            .alias("total_orders"),
            F.sum("_net_revenue")
            .cast("double")
            .alias("total_revenue"),
            (
                F.sum("_net_revenue")
                / F.countDistinct("id_transacao")
            )
            .cast("double")
            .alias("avg_ticket"),
            F.max("data_pedido").alias("_last_order_date"),
            F.countDistinct("sku")
            .cast("long")
            .alias("distinct_products"),
        )
        .select(
            customer_key().alias("customer_key"),
            F.lit(as_of_date).cast("date").alias("as_of_date"),
            "total_orders",
            "total_revenue",
            "avg_ticket",
            F.datediff(
                F.lit(as_of_date).cast("date"),
                F.col("_last_order_date"),
            )
            .cast("long")
            .alias("recency_days"),
            "distinct_products",
        )
    )


def build_product_metrics(silver_df: DataFrame) -> DataFrame:
    base = silver_df.withColumn("_net_revenue", net_revenue_expression())

    return (
        base
        .groupBy("sku")
        .agg(
            F.max("categoria").alias("category"),
            F.countDistinct("id_transacao")
            .cast("long")
            .alias("total_orders"),
            F.sum("quantidade")
            .cast("long")
            .alias("units_sold"),
            F.sum("_net_revenue")
            .cast("double")
            .alias("total_revenue"),
            F.avg("preco_unitario")
            .cast("double")
            .alias("avg_unit_price"),
            F.countDistinct("id_cliente_final")
            .cast("long")
            .alias("distinct_customers"),
        )
        .select(
            product_key().alias("product_key"),
            "sku",
            "category",
            "total_orders",
            "units_sold",
            "total_revenue",
            "avg_unit_price",
            "distinct_customers",
        )
    )


def expected_columns(contract: dict, dataset: str) -> list[str]:
    return list(contract["datasets"][dataset]["columns"])


def validate_exact_columns(
    dataframe: DataFrame,
    dataset: str,
    contract: dict,
) -> None:
    expected = expected_columns(contract, dataset)

    if dataframe.columns != expected:
        raise RuntimeError(
            f"{dataset} columns differ from ML contract. "
            f"Expected={expected}, actual={dataframe.columns}"
        )


def validate_unique_key(
    dataframe: DataFrame,
    dataset: str,
    key_column: str,
) -> None:
    if dataframe.where(F.col(key_column).isNull()).limit(1).count():
        raise RuntimeError(f"{dataset}.{key_column} contains NULL.")

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
            f"{dataset}.{key_column} is not unique."
        )


def validate_privacy(
    dataframes: dict[str, DataFrame],
    contract: dict,
) -> None:
    forbidden = {
        str(value).lower()
        for value in contract["privacy"]["forbidden_columns"]
    }

    for dataset, dataframe in dataframes.items():
        exposed = forbidden.intersection(
            {column.lower() for column in dataframe.columns}
        )

        if exposed:
            raise RuntimeError(
                f"Forbidden PII exposed by {dataset}: "
                + ", ".join(sorted(exposed))
            )


def validate_non_negative_features(
    customer_features_df: DataFrame,
) -> None:
    invalid = (
        customer_features_df
        .where(
            (F.col("total_orders") < 0)
            | (F.col("total_revenue") < 0)
            | (F.col("avg_ticket") < 0)
            | (F.col("recency_days") < 0)
            | (F.col("distinct_products") < 0)
        )
        .limit(1)
        .count()
    )

    if invalid:
        raise RuntimeError(
            "Customer feature dataset contains negative features."
        )


def train_segmentation(
    customer_features_df: DataFrame,
    contract: dict,
):
    model_contract = contract["model"]
    feature_columns = list(model_contract["feature_columns"])

    training_pdf = (
        customer_features_df
        .select("customer_key", *feature_columns)
        .orderBy("customer_key")
        .toPandas()
    )

    if len(training_pdf) <= int(model_contract["n_clusters"]):
        raise RuntimeError(
            "Not enough customers to train the configured KMeans model."
        )

    x = training_pdf[feature_columns].astype("float64")

    if x.isnull().any().any():
        raise RuntimeError("ML feature matrix contains NULL/NaN values.")

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "kmeans",
                KMeans(
                    n_clusters=int(model_contract["n_clusters"]),
                    random_state=int(model_contract["random_state"]),
                    n_init=int(model_contract["n_init"]),
                ),
            ),
        ]
    )

    labels = pipeline.fit_predict(x)

    scaled_features = pipeline.named_steps["scaler"].transform(x)
    silhouette = float(
        silhouette_score(scaled_features, labels)
    )
    inertia = float(
        pipeline.named_steps["kmeans"].inertia_
    )

    segment_rows = [
        (int(customer_key_value), int(segment_id))
        for customer_key_value, segment_id
        in zip(training_pdf["customer_key"], labels, strict=True)
    ]

    segment_ids_df = spark.createDataFrame(
        segment_rows,
        "customer_key LONG, segment_id INT",
    )

    segments_df = (
        customer_features_df
        .drop("as_of_date")
        .join(segment_ids_df, "customer_key", "inner")
        .select(
            "customer_key",
            "segment_id",
            *feature_columns,
        )
    )

    cluster_counts = {
        int(row["segment_id"]): int(row["count"])
        for row in (
            segments_df
            .groupBy("segment_id")
            .count()
            .orderBy("segment_id")
            .collect()
        )
    }

    return pipeline, x, segments_df, silhouette, inertia, cluster_counts


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
    ml_contract_sha256: str,
    contract_version: str,
    runtime_signature: str,
    mlflow_run_id: str,
    model_uri: str,
    counts: dict[str, int],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {STATE_TABLE} AS target
        USING (
            SELECT
                '{sql_string(ML_LAYER_NAME)}'
                    AS layer_name,
                '{sql_string(input_signature)}'
                    AS input_signature,
                '{sql_string(silver_input_signature)}'
                    AS silver_input_signature,
                '{sql_string(ml_contract_sha256)}'
                    AS ml_contract_sha256,
                '{sql_string(contract_version)}'
                    AS contract_version,
                '{sql_string(runtime_signature)}'
                    AS runtime_fingerprint,
                '{sql_string(mlflow_run_id)}'
                    AS mlflow_run_id,
                '{sql_string(model_uri)}'
                    AS model_uri,
                '{sql_string(RUN_ID)}'
                    AS last_successful_run_id,
                current_timestamp()
                    AS last_successful_at_utc,
                CAST({counts["customer_features_rows"]} AS BIGINT)
                    AS customer_features_rows,
                CAST({counts["product_metrics_rows"]} AS BIGINT)
                    AS product_metrics_rows,
                CAST({counts["customer_segments_rows"]} AS BIGINT)
                    AS customer_segments_rows
        ) AS source
        ON target.layer_name = source.layer_name
        WHEN MATCHED THEN UPDATE SET
            target.input_signature =
                source.input_signature,
            target.silver_input_signature =
                source.silver_input_signature,
            target.ml_contract_sha256 =
                source.ml_contract_sha256,
            target.contract_version =
                source.contract_version,
            target.runtime_fingerprint =
                source.runtime_fingerprint,
            target.mlflow_run_id =
                source.mlflow_run_id,
            target.model_uri =
                source.model_uri,
            target.last_successful_run_id =
                source.last_successful_run_id,
            target.last_successful_at_utc =
                source.last_successful_at_utc,
            target.customer_features_rows =
                source.customer_features_rows,
            target.product_metrics_rows =
                source.product_metrics_rows,
            target.customer_segments_rows =
                source.customer_segments_rows
        WHEN NOT MATCHED THEN INSERT (
            layer_name,
            input_signature,
            silver_input_signature,
            ml_contract_sha256,
            contract_version,
            runtime_fingerprint,
            mlflow_run_id,
            model_uri,
            last_successful_run_id,
            last_successful_at_utc,
            customer_features_rows,
            product_metrics_rows,
            customer_segments_rows
        )
        VALUES (
            source.layer_name,
            source.input_signature,
            source.silver_input_signature,
            source.ml_contract_sha256,
            source.contract_version,
            source.runtime_fingerprint,
            source.mlflow_run_id,
            source.model_uri,
            source.last_successful_run_id,
            source.last_successful_at_utc,
            source.customer_features_rows,
            source.product_metrics_rows,
            source.customer_segments_rows
        )
        """
    )


# COMMAND ----------

log_event("gold_ml_run_started", catalog=CATALOG)

contract, ml_contract_sha256 = load_contract()
ensure_state_table()

silver_state = read_silver_state()
silver_input_signature = str(silver_state["input_signature"])

health = validate_observability_gate(
    silver_input_signature,
    contract,
)

runtime_signature = runtime_fingerprint()

input_signature = compute_input_signature(
    silver_input_signature,
    ml_contract_sha256,
    runtime_signature,
)

state = previous_state()

if (
    state is not None
    and state["input_signature"] == input_signature
    and targets_are_consistent(state)
):
    result = {
        "run_id": RUN_ID,
        "status": "SKIPPED",
        "reason": "unchanged_silver_input_ml_contract_and_runtime",
        "health_status": health["health_status"],
        "customer_features_rows": int(state["customer_features_rows"]),
        "product_metrics_rows": int(state["product_metrics_rows"]),
        "customer_segments_rows": int(state["customer_segments_rows"]),
        "mlflow_run_id": state["mlflow_run_id"],
        "model_uri": state["model_uri"],
        "contract_version": str(contract["contract_version"]),
        "runtime_fingerprint": runtime_signature,
        "input_signature": input_signature,
    }

    log_event("gold_ml_run_skipped", **result)
    dbutils.notebook.exit(json.dumps(result))

silver_df = spark.table(SILVER_TABLE)

customer_features_df = build_customer_features(silver_df)
product_metrics_df = build_product_metrics(silver_df)

validate_exact_columns(
    customer_features_df,
    "customer_features",
    contract,
)
validate_exact_columns(
    product_metrics_df,
    "product_metrics",
    contract,
)

validate_unique_key(
    customer_features_df,
    "customer_features",
    "customer_key",
)
validate_unique_key(
    product_metrics_df,
    "product_metrics",
    "product_key",
)

validate_non_negative_features(customer_features_df)

validate_privacy(
    {
        "customer_features": customer_features_df,
        "product_metrics": product_metrics_df,
    },
    contract,
)

(
    sklearn_pipeline,
    training_x,
    customer_segments_df,
    silhouette,
    inertia,
    cluster_counts,
) = train_segmentation(
    customer_features_df,
    contract,
)

validate_exact_columns(
    customer_segments_df,
    "customer_segments",
    contract,
)
validate_unique_key(
    customer_segments_df,
    "customer_segments",
    "customer_key",
)

validate_privacy(
    {"customer_segments": customer_segments_df},
    contract,
)

customer_features_rows = customer_features_df.count()
product_metrics_rows = product_metrics_df.count()
customer_segments_rows = customer_segments_df.count()

if customer_features_rows != customer_segments_rows:
    raise RuntimeError(
        "Customer features and segments row counts do not match: "
        f"features={customer_features_rows}, "
        f"segments={customer_segments_rows}."
    )

safe_run_id = RUN_ID.replace("-", "_").replace(":", "_")

customer_features_candidate = (
    f"{CATALOG}.gold_ml._customer_features_candidate_{safe_run_id}"
)
product_metrics_candidate = (
    f"{CATALOG}.gold_ml._product_metrics_candidate_{safe_run_id}"
)
customer_segments_candidate = (
    f"{CATALOG}.gold_ml._customer_segments_candidate_{safe_run_id}"
)

candidate_tables = [
    customer_features_candidate,
    product_metrics_candidate,
    customer_segments_candidate,
]

experiment_suffix = str(contract["mlflow"]["experiment_suffix"])
current_user = spark.sql("SELECT current_user()").first()[0]
experiment_name = f"/Users/{current_user}/{experiment_suffix}"

mlflow.set_experiment(experiment_name)

mlflow_run_id = None
model_uri = None

try:
    write_candidate(
        customer_features_df,
        customer_features_candidate,
    )
    write_candidate(
        product_metrics_df,
        product_metrics_candidate,
    )
    write_candidate(
        customer_segments_df,
        customer_segments_candidate,
    )

    expected_counts = {
        customer_features_candidate: customer_features_rows,
        product_metrics_candidate: product_metrics_rows,
        customer_segments_candidate: customer_segments_rows,
    }

    for candidate_table, expected_rows in expected_counts.items():
        actual_rows = spark.table(candidate_table).count()

        if actual_rows != expected_rows:
            raise RuntimeError(
                f"Candidate read-back failed for {candidate_table}: "
                f"expected={expected_rows}, actual={actual_rows}"
            )

    with mlflow.start_run(
        run_name=f"customer-segmentation-{RUN_ID}"
    ) as active_run:
        mlflow_run_id = active_run.info.run_id

        mlflow.set_tags(
            {
                "project": "databricks-customer-lakehouse",
                "layer": ML_LAYER_NAME,
                "publication_status": "CANDIDATE",
                "silver_input_signature": silver_input_signature,
                "ml_contract_sha256": ml_contract_sha256,
                "pipeline_run_id": RUN_ID,
                "runtime_fingerprint": runtime_signature,
            }
        )

        mlflow.log_params(
            {
                "algorithm": contract["model"]["algorithm"],
                "n_clusters": int(
                    contract["model"]["n_clusters"]
                ),
                "random_state": int(
                    contract["model"]["random_state"]
                ),
                "n_init": int(contract["model"]["n_init"]),
                "scaler": contract["model"]["scaler"],
                "feature_count": len(
                    contract["model"]["feature_columns"]
                ),
            }
        )

        metrics = {
            "silhouette_score": silhouette,
            "inertia": inertia,
            "customer_count": float(customer_features_rows),
            "product_count": float(product_metrics_rows),
        }

        for segment_id, count in cluster_counts.items():
            metrics[f"cluster_{segment_id}_count"] = float(count)

        mlflow.log_metrics(metrics)
        mlflow.log_dict(contract, "contracts/ml_contract.json")

        input_example = training_x.head(
            min(5, len(training_x))
        )

        model_info = mlflow.sklearn.log_model(
            sk_model=sklearn_pipeline,
            name="model",
            input_example=input_example,
        )

        model_uri = model_info.model_uri

        # Publish only after the model has been logged successfully.
        publish_table(
            customer_features_candidate,
            CUSTOMER_FEATURES_TABLE,
            "Privacy-aware customer ML features",
        )
        publish_table(
            product_metrics_candidate,
            PRODUCT_METRICS_TABLE,
            "Product-level ML metrics",
        )
        publish_table(
            customer_segments_candidate,
            CUSTOMER_SEGMENTS_TABLE,
            "KMeans customer segmentation output; segment IDs are categorical",
        )

        published_counts = {
            "customer_features_rows":
                spark.table(CUSTOMER_FEATURES_TABLE).count(),
            "product_metrics_rows":
                spark.table(PRODUCT_METRICS_TABLE).count(),
            "customer_segments_rows":
                spark.table(CUSTOMER_SEGMENTS_TABLE).count(),
        }

        expected_published_counts = {
            "customer_features_rows": customer_features_rows,
            "product_metrics_rows": product_metrics_rows,
            "customer_segments_rows": customer_segments_rows,
        }

        if published_counts != expected_published_counts:
            raise RuntimeError(
                "Published Gold ML counts differ from validated candidates."
            )

        validate_privacy(
            {
                "customer_features":
                    spark.table(CUSTOMER_FEATURES_TABLE),
                "product_metrics":
                    spark.table(PRODUCT_METRICS_TABLE),
                "customer_segments":
                    spark.table(CUSTOMER_SEGMENTS_TABLE),
            },
            contract,
        )

        persist_state(
            input_signature=input_signature,
            silver_input_signature=silver_input_signature,
            ml_contract_sha256=ml_contract_sha256,
            contract_version=str(contract["contract_version"]),
            runtime_signature=runtime_signature,
            mlflow_run_id=mlflow_run_id,
            model_uri=model_uri,
            counts=published_counts,
        )

        mlflow.set_tag("publication_status", "PUBLISHED")

finally:
    for candidate_table in candidate_tables:
        spark.sql(f"DROP TABLE IF EXISTS {candidate_table}")


result = {
    "run_id": RUN_ID,
    "status": "PROCESSED",
    "health_status": health["health_status"],
    "customer_features_rows": customer_features_rows,
    "product_metrics_rows": product_metrics_rows,
    "customer_segments_rows": customer_segments_rows,
    "n_clusters": int(contract["model"]["n_clusters"]),
    "silhouette_score": round(silhouette, 6),
    "inertia": round(inertia, 6),
    "cluster_counts": cluster_counts,
    "mlflow_experiment": experiment_name,
    "mlflow_run_id": mlflow_run_id,
    "model_uri": model_uri,
    "contract_version": str(contract["contract_version"]),
    "ml_contract_sha256": ml_contract_sha256,
    "silver_input_signature": silver_input_signature,
    "runtime_fingerprint": runtime_signature,
    "input_signature": input_signature,
}

log_event("gold_ml_run_completed", **result)
print(json.dumps(result, indent=2, ensure_ascii=False))
print("Evidence level: ENVIRONMENT VALIDATED")

dbutils.notebook.exit(json.dumps(result))
