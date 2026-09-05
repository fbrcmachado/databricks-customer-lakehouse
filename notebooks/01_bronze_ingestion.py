# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Incrementally ingests immutable CSV source files from the Unity Catalog
# MAGIC Landing Volume into a Delta Bronze table.
# MAGIC
# MAGIC Execution semantics:
# MAGIC - new file/hash: process;
# MAGIC - same file/hash: skip;
# MAGIC - same file with a different hash: fail (Landing immutability violation);
# MAGIC - retry after partial failure: overwrite only that file's Bronze partition,
# MAGIC   then update the ingestion manifest.

# COMMAND ----------

from __future__ import annotations

import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
dbutils.widgets.text("expected_file_count", "50")

CATALOG = dbutils.widgets.get("catalog").strip()
EXPECTED_FILE_COUNT = int(dbutils.widgets.get("expected_file_count"))

LANDING_VOLUME = f"/Volumes/{CATALOG}/landing/source_files"
SOURCE_MANIFEST_PATH = f"{LANDING_VOLUME}/_meta/source_manifest.json"

BRONZE_TABLE = f"{CATALOG}.bronze.sales_raw"
INGESTION_MANIFEST_TABLE = f"{CATALOG}.observability.ingestion_manifest"

BRONZE_SCHEMA_VERSION = "1.0.0"

RUN_ID = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    + "_"
    + uuid.uuid4().hex[:8]
)

# COMMAND ----------

def log_event(event: str, **details: object) -> None:
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "event": event,
        **details,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))


def quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def detect_delimiter(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except csv.Error:
        comma_count = sample.count(",")
        semicolon_count = sample.count(";")
        return ";" if semicolon_count > comma_count else ","


def load_source_manifest() -> list[dict[str, object]]:
    manifest_path = Path(SOURCE_MANIFEST_PATH)

    if not manifest_path.exists():
        raise RuntimeError(
            f"Source manifest does not exist: {SOURCE_MANIFEST_PATH}"
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files", [])

    if len(files) != EXPECTED_FILE_COUNT:
        raise RuntimeError(
            "Source manifest contract violation: "
            f"expected {EXPECTED_FILE_COUNT} files, found {len(files)}."
        )

    names = [str(item["file_name"]) for item in files]

    if len(names) != len(set(names)):
        raise RuntimeError("Source manifest contains duplicate file names.")

    return sorted(files, key=lambda item: str(item["file_name"]))


def ensure_tables() -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            payload MAP<STRING, STRING>,
            _source_file STRING,
            _source_path STRING,
            _file_sha256 STRING,
            _source_row_number BIGINT,
            _ingested_at_utc TIMESTAMP,
            _run_id STRING,
            _bronze_schema_version STRING
        )
        USING DELTA
        PARTITIONED BY (_source_file)
        COMMENT 'Raw immutable sales records preserving source columns as a string map'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {INGESTION_MANIFEST_TABLE} (
            source_file STRING,
            source_path STRING,
            sha256 STRING,
            size_bytes BIGINT,
            row_count BIGINT,
            status STRING,
            first_ingested_at_utc TIMESTAMP,
            last_ingested_at_utc TIMESTAMP,
            last_run_id STRING,
            bronze_schema_version STRING
        )
        USING DELTA
        COMMENT 'Persistent ingestion state for immutable Landing source files'
        """
    )


def manifest_state() -> dict[str, dict[str, object]]:
    rows = spark.table(INGESTION_MANIFEST_TABLE).collect()
    return {
        row["source_file"]: row.asDict(recursive=True)
        for row in rows
    }


def read_csv_as_bronze(
    source_path: str,
    source_file: str,
    sha256: str,
) -> DataFrame:
    delimiter = detect_delimiter(source_path)

    source_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("sep", delimiter)
        .csv(source_path)
    )

    if not source_df.columns:
        raise RuntimeError(f"No columns found in source file: {source_file}")

    if len(source_df.columns) != len(set(source_df.columns)):
        raise RuntimeError(
            f"Duplicate source column names are not supported: {source_file}"
        )

    payload_keys = F.array(
        *[F.lit(column_name) for column_name in source_df.columns]
    )
    payload_values = F.array(
        *[source_df[column_name].cast("string") for column_name in source_df.columns]
    )

    row_window = Window.orderBy(F.monotonically_increasing_id())

    return (
        source_df
        .select(F.map_from_arrays(payload_keys, payload_values).alias("payload"))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_path", F.lit(source_path))
        .withColumn("_file_sha256", F.lit(sha256))
        .withColumn(
            "_source_row_number",
            F.row_number().over(row_window).cast("long"),
        )
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
        .withColumn(
            "_bronze_schema_version",
            F.lit(BRONZE_SCHEMA_VERSION),
        )
        .select(
            "payload",
            "_source_file",
            "_source_path",
            "_file_sha256",
            "_source_row_number",
            "_ingested_at_utc",
            "_run_id",
            "_bronze_schema_version",
        )
    )


def publish_file_partition(
    bronze_df: DataFrame,
    source_file: str,
) -> int:
    row_count = bronze_df.count()

    if row_count <= 0:
        raise RuntimeError(f"Source file has no data rows: {source_file}")

    predicate = (
        "_source_file = "
        f"'{quote_sql_string(source_file)}'"
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(BRONZE_TABLE)
    )

    published_count = (
        spark.table(BRONZE_TABLE)
        .where(F.col("_source_file") == source_file)
        .count()
    )

    if published_count != row_count:
        raise RuntimeError(
            f"Bronze publication validation failed for {source_file}: "
            f"expected {row_count}, found {published_count}."
        )

    return row_count


def publish_manifest_entry(
    *,
    source_file: str,
    source_path: str,
    sha256: str,
    size_bytes: int,
    row_count: int,
) -> None:
    source_file_sql = quote_sql_string(source_file)
    source_path_sql = quote_sql_string(source_path)
    sha_sql = quote_sql_string(sha256)

    spark.sql(
        f"""
        MERGE INTO {INGESTION_MANIFEST_TABLE} AS target
        USING (
            SELECT
                '{source_file_sql}' AS source_file,
                '{source_path_sql}' AS source_path,
                '{sha_sql}' AS sha256,
                CAST({int(size_bytes)} AS BIGINT) AS size_bytes,
                CAST({int(row_count)} AS BIGINT) AS row_count,
                'active' AS status,
                current_timestamp() AS event_time,
                '{quote_sql_string(RUN_ID)}' AS run_id,
                '{BRONZE_SCHEMA_VERSION}' AS bronze_schema_version
        ) AS source
        ON target.source_file = source.source_file
        WHEN MATCHED THEN UPDATE SET
            target.source_path = source.source_path,
            target.sha256 = source.sha256,
            target.size_bytes = source.size_bytes,
            target.row_count = source.row_count,
            target.status = source.status,
            target.last_ingested_at_utc = source.event_time,
            target.last_run_id = source.run_id,
            target.bronze_schema_version = source.bronze_schema_version
        WHEN NOT MATCHED THEN INSERT (
            source_file,
            source_path,
            sha256,
            size_bytes,
            row_count,
            status,
            first_ingested_at_utc,
            last_ingested_at_utc,
            last_run_id,
            bronze_schema_version
        ) VALUES (
            source.source_file,
            source.source_path,
            source.sha256,
            source.size_bytes,
            source.row_count,
            source.status,
            source.event_time,
            source.event_time,
            source.run_id,
            source.bronze_schema_version
        )
        """
    )


def final_validation(expected_files: list[dict[str, object]]) -> dict[str, int]:
    expected_names = {str(item["file_name"]) for item in expected_files}

    manifest_df = spark.table(INGESTION_MANIFEST_TABLE).where(
        F.col("status") == "active"
    )

    manifest_names = {
        row["source_file"]
        for row in manifest_df.select("source_file").collect()
    }

    missing_manifest = expected_names - manifest_names

    if missing_manifest:
        raise RuntimeError(
            "Ingestion manifest is missing source files: "
            + ", ".join(sorted(missing_manifest))
        )

    bronze_df = spark.table(BRONZE_TABLE)

    bronze_names = {
        row["_source_file"]
        for row in bronze_df.select("_source_file").distinct().collect()
    }

    missing_bronze = expected_names - bronze_names

    if missing_bronze:
        raise RuntimeError(
            "Bronze table is missing source files: "
            + ", ".join(sorted(missing_bronze))
        )

    return {
        "bronze_rows": bronze_df.count(),
        "bronze_files": len(bronze_names),
        "manifest_files": manifest_df.count(),
    }

# COMMAND ----------

log_event(
    "bronze_run_started",
    catalog=CATALOG,
    expected_file_count=EXPECTED_FILE_COUNT,
)

source_files = load_source_manifest()
ensure_tables()
state = manifest_state()

processed_files = 0
skipped_files = 0
processed_rows = 0

for source in source_files:
    source_file = str(source["file_name"])
    sha256 = str(source["sha256"]).lower()
    size_bytes = int(source["size_bytes"])
    source_path = f"{LANDING_VOLUME}/{source_file}"

    existing = state.get(source_file)

    if existing is not None:
        existing_hash = str(existing["sha256"]).lower()

        if existing_hash == sha256:
            skipped_files += 1
            log_event(
                "source_file_skipped",
                source_file=source_file,
                reason="same_sha256",
            )
            continue

        raise RuntimeError(
            "Landing immutability violation: "
            f"{source_file} was previously ingested with SHA-256 "
            f"{existing_hash}, but the current source manifest contains {sha256}."
        )

    log_event(
        "source_file_processing",
        source_file=source_file,
        sha256=sha256,
    )

    bronze_df = read_csv_as_bronze(
        source_path=source_path,
        source_file=source_file,
        sha256=sha256,
    )

    row_count = publish_file_partition(
        bronze_df=bronze_df,
        source_file=source_file,
    )

    publish_manifest_entry(
        source_file=source_file,
        source_path=source_path,
        sha256=sha256,
        size_bytes=size_bytes,
        row_count=row_count,
    )

    state[source_file] = {
        "source_file": source_file,
        "sha256": sha256,
    }

    processed_files += 1
    processed_rows += row_count

    log_event(
        "source_file_processed",
        source_file=source_file,
        rows=row_count,
    )

summary = final_validation(source_files)

log_event(
    "bronze_run_completed",
    processed_files=processed_files,
    skipped_files=skipped_files,
    processed_rows=processed_rows,
    bronze_rows=summary["bronze_rows"],
    bronze_files=summary["bronze_files"],
    manifest_files=summary["manifest_files"],
)

print("")
print("Bronze ingestion complete.")
print("--------------------------")
print(f"Run ID:          {RUN_ID}")
print(f"Processed files: {processed_files}")
print(f"Skipped files:   {skipped_files}")
print(f"Processed rows:  {processed_rows}")
print(f"Bronze rows:     {summary['bronze_rows']}")
print(f"Bronze files:    {summary['bronze_files']}")
print(f"Manifest files:  {summary['manifest_files']}")
print("")
print("Evidence level: ENVIRONMENT VALIDATED")
