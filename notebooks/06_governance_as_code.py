# Databricks notebook source
# MAGIC %md
# MAGIC # Governance as Code + Unity Catalog
# MAGIC
# MAGIC Validates declared governance metadata against real Unity Catalog assets,
# MAGIC evaluates blocking policies, materializes governance evidence in Delta,
# MAGIC and synchronizes governed table descriptions into Unity Catalog comments.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import Row, functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "customer_lakehouse")
CATALOG = dbutils.widgets.get("catalog").strip()

SILVER_STATE_TABLE = f"{CATALOG}.observability.transformation_state"
GOLD_STATE_TABLE = f"{CATALOG}.observability.gold_publication_state"
ML_STATE_TABLE = f"{CATALOG}.observability.ml_publication_state"
LATEST_HEALTH_VIEW = f"{CATALOG}.observability.v_pipeline_health_latest"

SUMMARY_TABLE = f"{CATALOG}.governance.governance_summary"
CHECKS_TABLE = f"{CATALOG}.governance.governance_checks"
ASSETS_TABLE = f"{CATALOG}.governance.catalog_assets"
GLOSSARY_TABLE = f"{CATALOG}.governance.business_glossary"
OWNERSHIP_TABLE = f"{CATALOG}.governance.ownership"
CLASSIFICATIONS_TABLE = f"{CATALOG}.governance.classifications"
LINEAGE_TABLE = f"{CATALOG}.governance.lineage_edges"
STATE_TABLE = f"{CATALOG}.observability.governance_state"

GOVERNANCE_LAYER_NAME = "governance_as_code"
SILVER_LAYER_NAME = "silver_sales_transactions"
GOLD_LAYER_NAME = "gold_bi_star_schema"
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

GOVERNANCE_FILES = {
    "contract": PROJECT_ROOT / "config" / "governance_contract.yaml",
    "assets": PROJECT_ROOT / "governance" / "catalog" / "assets.yaml",
    "domains": PROJECT_ROOT / "governance" / "ownership" / "domains.yaml",
    "classification":
        PROJECT_ROOT
        / "governance"
        / "classification"
        / "data_classification.yaml",
    "pii": PROJECT_ROOT / "governance" / "policies" / "pii.yaml",
    "quality_sla":
        PROJECT_ROOT / "governance" / "policies" / "quality_sla.yaml",
    "retention":
        PROJECT_ROOT / "governance" / "policies" / "retention.yaml",
    "glossary":
        PROJECT_ROOT / "governance" / "glossary" / "business_terms.yaml",
    "lineage":
        PROJECT_ROOT / "governance" / "lineage" / "lineage.yaml",
}

# COMMAND ----------

def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(f"Governance file not found: {path}")

    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))

    if not isinstance(parsed, dict):
        raise RuntimeError(f"Governance file is not a mapping: {path}")

    return parsed


def load_governance() -> tuple[dict[str, dict], str]:
    documents = {
        key: load_yaml(path)
        for key, path in GOVERNANCE_FILES.items()
    }

    digest = hashlib.sha256()

    for key in sorted(GOVERNANCE_FILES):
        path = GOVERNANCE_FILES[key]
        digest.update(key.encode("utf-8"))
        digest.update(path.read_bytes())

    return documents, digest.hexdigest()


def read_state(table_name: str, layer_name: str) -> dict[str, object]:
    rows = (
        spark.table(table_name)
        .where(F.col("layer_name") == layer_name)
        .limit(1)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            f"Required state is missing: {table_name} / {layer_name}"
        )

    return rows[0].asDict(recursive=True)


def read_operational_evidence(documents: dict[str, dict]) -> dict[str, object]:
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

    health_row = spark.table(LATEST_HEALTH_VIEW).first()

    if health_row is None:
        raise RuntimeError("Latest observability health view is empty.")

    health = health_row.asDict(recursive=True)
    evidence = documents["contract"]["evidence"]

    if bool(evidence["require_current_observability_health"]):
        required_status = str(evidence["required_health_status"])

        if health["health_status"] != required_status:
            raise RuntimeError(
                "Governance cannot evaluate a stale/unhealthy data product: "
                f"health={health['health_status']}, "
                f"required={required_status}."
            )

    if bool(evidence["require_matching_silver_signature"]):
        silver_signature = str(silver_state["input_signature"])

        if health["source_input_signature"] != silver_signature:
            raise RuntimeError(
                "Observability evidence does not match current Silver state."
            )

        if gold_state["silver_input_signature"] != silver_signature:
            raise RuntimeError(
                "Gold BI state does not match current Silver state."
            )

        if ml_state["silver_input_signature"] != silver_signature:
            raise RuntimeError(
                "Gold ML state does not match current Silver state."
            )

    return {
        "silver_input_signature": str(silver_state["input_signature"]),
        "gold_input_signature": str(gold_state["input_signature"]),
        "ml_input_signature": str(ml_state["input_signature"]),
        "health_status": str(health["health_status"]),
        "valid_pct": float(health["valid_pct"]),
        "row_accounting_ok": bool(health["row_accounting_ok"]),
        "file_count_ok": bool(health["file_count_ok"]),
    }


def table_schema_fingerprint(object_name: str) -> dict[str, object]:
    exists = spark.catalog.tableExists(object_name)

    if not exists:
        return {
            "object_name": object_name,
            "exists": False,
            "columns": [],
        }

    schema = spark.table(object_name).schema

    return {
        "object_name": object_name,
        "exists": True,
        "columns": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in schema.fields
        ],
    }


def build_uc_inventory(
    documents: dict[str, dict],
) -> tuple[dict[str, dict[str, object]], str]:
    inventory = {}

    for asset in documents["assets"]["assets"]:
        object_name = str(asset["object_name"])
        inventory[asset["asset_id"]] = table_schema_fingerprint(object_name)

    encoded = json.dumps(
        inventory,
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")

    return inventory, hashlib.sha256(encoded).hexdigest()


def compute_input_signature(
    governance_sha256: str,
    uc_inventory_sha256: str,
    evidence: dict[str, object],
) -> str:
    payload = {
        "governance_sha256": governance_sha256,
        "uc_inventory_sha256": uc_inventory_sha256,
        **evidence,
    }

    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def ensure_tables() -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
            governance_signature STRING,
            governance_sha256 STRING,
            uc_inventory_sha256 STRING,
            run_id STRING,
            evaluated_at_utc TIMESTAMP,
            overall_status STRING,
            total_controls BIGINT,
            passed_controls BIGINT,
            failed_controls BIGINT,
            blocking_failures BIGINT,
            declared_assets BIGINT,
            existing_assets BIGINT,
            silver_input_signature STRING,
            gold_input_signature STRING,
            ml_input_signature STRING,
            health_status STRING,
            valid_pct DOUBLE
        )
        USING DELTA
        COMMENT 'Governance-as-Code evaluation summary'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CHECKS_TABLE} (
            governance_signature STRING,
            run_id STRING,
            evaluated_at_utc TIMESTAMP,
            control_id STRING,
            control_category STRING,
            blocking BOOLEAN,
            status STRING,
            asset_id STRING,
            details STRING
        )
        USING DELTA
        COMMENT 'Executable governance control results'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ASSETS_TABLE} (
            governance_signature STRING,
            asset_id STRING,
            object_name STRING,
            object_type STRING,
            domain STRING,
            owner_role STRING,
            steward_role STRING,
            classification STRING,
            description STRING,
            uc_exists BOOLEAN,
            schema_json STRING
        )
        USING DELTA
        COMMENT 'Declared data catalog joined with Unity Catalog runtime evidence'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GLOSSARY_TABLE} (
            governance_signature STRING,
            term STRING,
            definition STRING,
            domain STRING,
            related_assets ARRAY<STRING>
        )
        USING DELTA
        COMMENT 'Business glossary managed as code'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {OWNERSHIP_TABLE} (
            governance_signature STRING,
            domain STRING,
            domain_description STRING,
            owner_role STRING,
            steward_role STRING
        )
        USING DELTA
        COMMENT 'Logical data-domain ownership and stewardship'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CLASSIFICATIONS_TABLE} (
            governance_signature STRING,
            asset_id STRING,
            column_name STRING,
            classification STRING,
            pii_type STRING
        )
        USING DELTA
        COMMENT 'Column-level classification and PII metadata'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {LINEAGE_TABLE} (
            governance_signature STRING,
            upstream_asset_id STRING,
            downstream_asset_id STRING,
            transformation STRING
        )
        USING DELTA
        COMMENT 'Declared business/data-product lineage edges'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            layer_name STRING,
            input_signature STRING,
            governance_sha256 STRING,
            uc_inventory_sha256 STRING,
            last_successful_run_id STRING,
            last_successful_at_utc TIMESTAMP,
            overall_status STRING,
            total_controls BIGINT,
            blocking_failures BIGINT
        )
        USING DELTA
        COMMENT 'Successful Governance-as-Code execution state'
        """
    )


def add_check(
    checks: list[dict[str, object]],
    *,
    control_id: str,
    category: str,
    blocking: bool,
    passed: bool,
    details: str,
    asset_id: str | None = None,
) -> None:
    checks.append(
        {
            "control_id": control_id,
            "control_category": category,
            "blocking": blocking,
            "status": "PASS" if passed else "FAIL",
            "asset_id": asset_id,
            "details": details,
        }
    )


def validate_lineage_acyclic(
    asset_ids: set[str],
    edges: list[dict[str, str]],
) -> bool:
    incoming = {node: 0 for node in asset_ids}
    outgoing = {node: [] for node in asset_ids}

    for edge in edges:
        source = edge["from"]
        target = edge["to"]

        if source not in asset_ids or target not in asset_ids:
            return False

        outgoing[source].append(target)
        incoming[target] += 1

    ready = [node for node, count in incoming.items() if count == 0]
    visited = 0

    while ready:
        node = ready.pop()
        visited += 1

        for child in outgoing[node]:
            incoming[child] -= 1

            if incoming[child] == 0:
                ready.append(child)

    return visited == len(asset_ids)


def evaluate_controls(
    documents: dict[str, dict],
    inventory: dict[str, dict[str, object]],
    evidence: dict[str, object],
) -> list[dict[str, object]]:
    checks = []

    assets = documents["assets"]["assets"]
    ownership = documents["domains"]
    classifications = documents["classification"]
    glossary = documents["glossary"]["terms"]
    edges = documents["lineage"]["edges"]
    retention = documents["retention"]
    pii = documents["pii"]
    quality_sla = documents["quality_sla"]

    asset_ids = {asset["asset_id"] for asset in assets}
    domains = set(ownership["domains"])
    roles = set(ownership["roles"])
    valid_levels = set(classifications["levels"])

    for asset in assets:
        asset_id = asset["asset_id"]
        inv = inventory[asset_id]

        add_check(
            checks,
            control_id=f"CATALOG_EXISTS::{asset_id}",
            category="catalog",
            blocking=True,
            passed=bool(inv["exists"]),
            asset_id=asset_id,
            details=(
                f"Unity Catalog object {asset['object_name']} "
                + ("exists." if inv["exists"] else "is missing.")
            ),
        )

        add_check(
            checks,
            control_id=f"OWNERSHIP::{asset_id}",
            category="ownership",
            blocking=True,
            passed=(
                asset["domain"] in domains
                and asset["owner_role"] in roles
                and asset["steward_role"] in roles
            ),
            asset_id=asset_id,
            details=(
                f"domain={asset['domain']}, "
                f"owner_role={asset['owner_role']}, "
                f"steward_role={asset['steward_role']}"
            ),
        )

        add_check(
            checks,
            control_id=f"CLASSIFICATION::{asset_id}",
            category="classification",
            blocking=True,
            passed=asset["classification"] in valid_levels,
            asset_id=asset_id,
            details=f"classification={asset['classification']}",
        )

    for entry in classifications["column_classifications"]:
        asset_id = entry["asset_id"]
        column_name = entry["column"]

        exists = False

        if asset_id in inventory and inventory[asset_id]["exists"]:
            exists = column_name in {
                column["name"]
                for column in inventory[asset_id]["columns"]
            }

        add_check(
            checks,
            control_id=f"COLUMN_CLASS::{asset_id}::{column_name}",
            category="classification",
            blocking=True,
            passed=exists,
            asset_id=asset_id,
            details=(
                f"column={column_name}, "
                f"classification={entry['classification']}, "
                f"pii_type={entry['pii_type']}"
            ),
        )

    for rule in pii["rules"]:
        prefix = str(rule["asset_prefix"])
        forbidden = {str(value).lower() for value in rule["forbidden_columns"]}

        for asset in assets:
            asset_id = str(asset["asset_id"])

            if not asset_id.startswith(prefix):
                continue

            actual_columns = {
                str(column["name"]).lower()
                for column in inventory[asset_id]["columns"]
            }

            exposed = sorted(forbidden.intersection(actual_columns))

            add_check(
                checks,
                control_id=f"{rule['rule_id']}::{asset_id}",
                category="pii",
                blocking=bool(pii["blocking"]),
                passed=not exposed,
                asset_id=asset_id,
                details=(
                    "No forbidden direct PII exposed."
                    if not exposed
                    else "Forbidden columns exposed: " + ", ".join(exposed)
                ),
            )

    minimum_valid_pct = float(quality_sla["minimum_valid_row_pct"])
    quality_passed = (
        evidence["health_status"] == quality_sla["required_health_status"]
        and evidence["valid_pct"] >= minimum_valid_pct
        and (
            not bool(quality_sla["require_exact_row_accounting"])
            or evidence["row_accounting_ok"]
        )
        and (
            not bool(quality_sla["require_file_count_ok"])
            or evidence["file_count_ok"]
        )
    )

    add_check(
        checks,
        control_id="QUALITY_SLA::silver.sales_transactions",
        category="quality_sla",
        blocking=bool(quality_sla["blocking"]),
        passed=quality_passed,
        asset_id="silver.sales_transactions",
        details=(
            f"health={evidence['health_status']}, "
            f"valid_pct={evidence['valid_pct']:.6f}, "
            f"minimum={minimum_valid_pct:.6f}, "
            f"row_accounting_ok={evidence['row_accounting_ok']}, "
            f"file_count_ok={evidence['file_count_ok']}"
        ),
    )

    lineage_refs_valid = all(
        edge["from"] in asset_ids and edge["to"] in asset_ids
        for edge in edges
    )

    add_check(
        checks,
        control_id="LINEAGE_REFERENCES",
        category="lineage",
        blocking=True,
        passed=lineage_refs_valid,
        details="All lineage edges must reference declared assets.",
    )

    lineage_acyclic = (
        lineage_refs_valid
        and validate_lineage_acyclic(asset_ids, edges)
    )

    add_check(
        checks,
        control_id="LINEAGE_ACYCLIC",
        category="lineage",
        blocking=True,
        passed=lineage_acyclic,
        details="Declared asset lineage must remain acyclic.",
    )

    for term in glossary:
        related = set(term["related_assets"])
        passed = term["domain"] in domains and related <= asset_ids

        add_check(
            checks,
            control_id=f"GLOSSARY::{term['term']}",
            category="glossary",
            blocking=True,
            passed=passed,
            details=(
                f"domain={term['domain']}; "
                f"related_assets={sorted(related)}"
            ),
        )

    retention_asset_ids = {
        rule["asset_id"]
        for rule in retention["rules"]
    }

    retention_refs_valid = retention_asset_ids <= asset_ids
    retention_days_valid = all(
        int(rule["retention_days"]) > 0
        for rule in retention["rules"]
    )
    destructive_disabled = (
        retention["enforcement_mode"] == "report_only"
        and retention["destructive_enforcement_enabled"] is False
    )

    add_check(
        checks,
        control_id="RETENTION_REFERENCES",
        category="retention",
        blocking=True,
        passed=retention_refs_valid and retention_days_valid,
        details="Retention rules reference declared assets with positive durations.",
    )

    add_check(
        checks,
        control_id="RETENTION_NON_DESTRUCTIVE",
        category="retention",
        blocking=True,
        passed=destructive_disabled,
        details=(
            f"mode={retention['enforcement_mode']}, "
            "destructive_enforcement_enabled="
            f"{retention['destructive_enforcement_enabled']}"
        ),
    )

    return checks


def synchronize_uc_comments(
    documents: dict[str, dict],
    inventory: dict[str, dict[str, object]],
) -> int:
    if not bool(documents["contract"]["runtime"]["apply_uc_comments"]):
        return 0

    applied = 0

    for asset in documents["assets"]["assets"]:
        asset_id = asset["asset_id"]

        if not inventory[asset_id]["exists"]:
            continue

        spark.sql(
            f"""
            COMMENT ON TABLE {asset["object_name"]}
            IS '{sql_string(str(asset["description"]))}'
            """
        )
        applied += 1

    return applied


def replace_signature_rows(
    table_name: str,
    governance_signature: str,
    dataframe,
) -> None:
    spark.sql(
        f"""
        DELETE FROM {table_name}
        WHERE governance_signature =
            '{sql_string(governance_signature)}'
        """
    )

    dataframe.write.mode("append").saveAsTable(table_name)


def publish_metadata(
    documents: dict[str, dict],
    inventory: dict[str, dict[str, object]],
    checks: list[dict[str, object]],
    governance_signature: str,
    governance_sha256: str,
    uc_inventory_sha256: str,
    evidence: dict[str, object],
) -> dict[str, object]:
    evaluated_at = datetime.now(timezone.utc)

    asset_rows = []

    for asset in documents["assets"]["assets"]:
        inv = inventory[asset["asset_id"]]

        asset_rows.append(
            (
                governance_signature,
                asset["asset_id"],
                asset["object_name"],
                asset["object_type"],
                asset["domain"],
                asset["owner_role"],
                asset["steward_role"],
                asset["classification"],
                asset["description"],
                bool(inv["exists"]),
                json.dumps(inv["columns"], sort_keys=True),
            )
        )

    assets_df = spark.createDataFrame(
        asset_rows,
        """
        governance_signature STRING,
        asset_id STRING,
        object_name STRING,
        object_type STRING,
        domain STRING,
        owner_role STRING,
        steward_role STRING,
        classification STRING,
        description STRING,
        uc_exists BOOLEAN,
        schema_json STRING
        """,
    )

    glossary_rows = [
        (
            governance_signature,
            term["term"],
            term["definition"],
            term["domain"],
            list(term["related_assets"]),
        )
        for term in documents["glossary"]["terms"]
    ]

    glossary_df = spark.createDataFrame(
        glossary_rows,
        """
        governance_signature STRING,
        term STRING,
        definition STRING,
        domain STRING,
        related_assets ARRAY<STRING>
        """,
    )

    ownership_rows = [
        (
            governance_signature,
            domain,
            spec["description"],
            spec["owner_role"],
            spec["steward_role"],
        )
        for domain, spec in documents["domains"]["domains"].items()
    ]

    ownership_df = spark.createDataFrame(
        ownership_rows,
        """
        governance_signature STRING,
        domain STRING,
        domain_description STRING,
        owner_role STRING,
        steward_role STRING
        """,
    )

    classification_rows = [
        (
            governance_signature,
            entry["asset_id"],
            entry["column"],
            entry["classification"],
            entry["pii_type"],
        )
        for entry in documents["classification"]["column_classifications"]
    ]

    classifications_df = spark.createDataFrame(
        classification_rows,
        """
        governance_signature STRING,
        asset_id STRING,
        column_name STRING,
        classification STRING,
        pii_type STRING
        """,
    )

    lineage_rows = [
        (
            governance_signature,
            edge["from"],
            edge["to"],
            edge["transformation"],
        )
        for edge in documents["lineage"]["edges"]
    ]

    lineage_df = spark.createDataFrame(
        lineage_rows,
        """
        governance_signature STRING,
        upstream_asset_id STRING,
        downstream_asset_id STRING,
        transformation STRING
        """,
    )

    check_rows = [
        (
            governance_signature,
            RUN_ID,
            evaluated_at,
            check["control_id"],
            check["control_category"],
            bool(check["blocking"]),
            check["status"],
            check["asset_id"],
            check["details"],
        )
        for check in checks
    ]

    checks_df = spark.createDataFrame(
        check_rows,
        """
        governance_signature STRING,
        run_id STRING,
        evaluated_at_utc TIMESTAMP,
        control_id STRING,
        control_category STRING,
        blocking BOOLEAN,
        status STRING,
        asset_id STRING,
        details STRING
        """,
    )

    total_controls = len(checks)
    failed_controls = sum(check["status"] == "FAIL" for check in checks)
    passed_controls = total_controls - failed_controls
    blocking_failures = sum(
        check["status"] == "FAIL" and bool(check["blocking"])
        for check in checks
    )
    existing_assets = sum(
        bool(value["exists"])
        for value in inventory.values()
    )
    declared_assets = len(inventory)
    overall_status = (
        "PASS"
        if blocking_failures == 0
        else "FAIL"
    )

    summary_df = spark.createDataFrame(
        [
            (
                governance_signature,
                governance_sha256,
                uc_inventory_sha256,
                RUN_ID,
                evaluated_at,
                overall_status,
                total_controls,
                passed_controls,
                failed_controls,
                blocking_failures,
                declared_assets,
                existing_assets,
                evidence["silver_input_signature"],
                evidence["gold_input_signature"],
                evidence["ml_input_signature"],
                evidence["health_status"],
                float(evidence["valid_pct"]),
            )
        ],
        """
        governance_signature STRING,
        governance_sha256 STRING,
        uc_inventory_sha256 STRING,
        run_id STRING,
        evaluated_at_utc TIMESTAMP,
        overall_status STRING,
        total_controls BIGINT,
        passed_controls BIGINT,
        failed_controls BIGINT,
        blocking_failures BIGINT,
        declared_assets BIGINT,
        existing_assets BIGINT,
        silver_input_signature STRING,
        gold_input_signature STRING,
        ml_input_signature STRING,
        health_status STRING,
        valid_pct DOUBLE
        """,
    )

    replace_signature_rows(
        ASSETS_TABLE,
        governance_signature,
        assets_df,
    )
    replace_signature_rows(
        GLOSSARY_TABLE,
        governance_signature,
        glossary_df,
    )
    replace_signature_rows(
        OWNERSHIP_TABLE,
        governance_signature,
        ownership_df,
    )
    replace_signature_rows(
        CLASSIFICATIONS_TABLE,
        governance_signature,
        classifications_df,
    )
    replace_signature_rows(
        LINEAGE_TABLE,
        governance_signature,
        lineage_df,
    )
    replace_signature_rows(
        CHECKS_TABLE,
        governance_signature,
        checks_df,
    )
    replace_signature_rows(
        SUMMARY_TABLE,
        governance_signature,
        summary_df,
    )

    return {
        "overall_status": overall_status,
        "total_controls": total_controls,
        "passed_controls": passed_controls,
        "failed_controls": failed_controls,
        "blocking_failures": blocking_failures,
        "declared_assets": declared_assets,
        "existing_assets": existing_assets,
    }


def previous_state() -> dict[str, object] | None:
    rows = (
        spark.table(STATE_TABLE)
        .where(F.col("layer_name") == GOVERNANCE_LAYER_NAME)
        .limit(1)
        .collect()
    )

    return rows[0].asDict(recursive=True) if rows else None


def evidence_snapshot_exists(governance_signature: str) -> bool:
    return (
        spark.table(SUMMARY_TABLE)
        .where(
            F.col("governance_signature")
            == governance_signature
        )
        .limit(1)
        .count()
        > 0
    )


def persist_state(
    *,
    input_signature: str,
    governance_sha256: str,
    uc_inventory_sha256: str,
    summary: dict[str, object],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {STATE_TABLE} AS target
        USING (
            SELECT
                '{sql_string(GOVERNANCE_LAYER_NAME)}'
                    AS layer_name,
                '{sql_string(input_signature)}'
                    AS input_signature,
                '{sql_string(governance_sha256)}'
                    AS governance_sha256,
                '{sql_string(uc_inventory_sha256)}'
                    AS uc_inventory_sha256,
                '{sql_string(RUN_ID)}'
                    AS last_successful_run_id,
                current_timestamp()
                    AS last_successful_at_utc,
                '{sql_string(str(summary["overall_status"]))}'
                    AS overall_status,
                CAST({summary["total_controls"]} AS BIGINT)
                    AS total_controls,
                CAST({summary["blocking_failures"]} AS BIGINT)
                    AS blocking_failures
        ) AS source
        ON target.layer_name = source.layer_name
        WHEN MATCHED THEN UPDATE SET
            target.input_signature = source.input_signature,
            target.governance_sha256 = source.governance_sha256,
            target.uc_inventory_sha256 = source.uc_inventory_sha256,
            target.last_successful_run_id = source.last_successful_run_id,
            target.last_successful_at_utc = source.last_successful_at_utc,
            target.overall_status = source.overall_status,
            target.total_controls = source.total_controls,
            target.blocking_failures = source.blocking_failures
        WHEN NOT MATCHED THEN INSERT (
            layer_name,
            input_signature,
            governance_sha256,
            uc_inventory_sha256,
            last_successful_run_id,
            last_successful_at_utc,
            overall_status,
            total_controls,
            blocking_failures
        )
        VALUES (
            source.layer_name,
            source.input_signature,
            source.governance_sha256,
            source.uc_inventory_sha256,
            source.last_successful_run_id,
            source.last_successful_at_utc,
            source.overall_status,
            source.total_controls,
            source.blocking_failures
        )
        """
    )


# COMMAND ----------

log_event("governance_run_started", catalog=CATALOG)

documents, governance_sha256 = load_governance()

expected_catalog = str(
    documents["contract"]["runtime"]["expected_catalog"]
)

if CATALOG != expected_catalog:
    raise RuntimeError(
        f"Governance contract expects catalog={expected_catalog}, "
        f"but runtime catalog={CATALOG}."
    )

ensure_tables()

evidence = read_operational_evidence(documents)
inventory, uc_inventory_sha256 = build_uc_inventory(documents)

governance_signature = compute_input_signature(
    governance_sha256,
    uc_inventory_sha256,
    evidence,
)

state = previous_state()

if (
    state is not None
    and state["input_signature"] == governance_signature
    and state["overall_status"] == "PASS"
    and evidence_snapshot_exists(governance_signature)
):
    result = {
        "run_id": RUN_ID,
        "status": "SKIPPED",
        "reason": "unchanged_governance_uc_inventory_and_data_product_state",
        "overall_status": state["overall_status"],
        "total_controls": int(state["total_controls"]),
        "blocking_failures": int(state["blocking_failures"]),
        "governance_signature": governance_signature,
        "governance_sha256": governance_sha256,
        "uc_inventory_sha256": uc_inventory_sha256,
        **evidence,
    }

    log_event("governance_run_skipped", **result)
    dbutils.notebook.exit(json.dumps(result))

checks = evaluate_controls(
    documents,
    inventory,
    evidence,
)

comments_applied = synchronize_uc_comments(
    documents,
    inventory,
)

summary = publish_metadata(
    documents,
    inventory,
    checks,
    governance_signature,
    governance_sha256,
    uc_inventory_sha256,
    evidence,
)

if summary["blocking_failures"] == 0:
    persist_state(
        input_signature=governance_signature,
        governance_sha256=governance_sha256,
        uc_inventory_sha256=uc_inventory_sha256,
        summary=summary,
    )

result = {
    "run_id": RUN_ID,
    "status": "PROCESSED",
    **summary,
    "comments_applied": comments_applied,
    "governance_signature": governance_signature,
    "governance_sha256": governance_sha256,
    "uc_inventory_sha256": uc_inventory_sha256,
    **evidence,
}

log_event("governance_run_completed", **result)
print(json.dumps(result, indent=2, ensure_ascii=False))

if (
    summary["blocking_failures"] > 0
    and bool(
        documents["contract"]["runtime"]["fail_on_blocking_control"]
    )
):
    raise RuntimeError(
        "Governance gate failed with "
        f"{summary['blocking_failures']} blocking control failure(s). "
        f"Evidence was persisted to {CHECKS_TABLE}."
    )

print("Evidence level: ENVIRONMENT VALIDATED")
dbutils.notebook.exit(json.dumps(result))
