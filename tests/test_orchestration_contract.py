from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
CONTRACT_PATH = ROOT / "config" / "orchestration_contract.yaml"
RESOURCE_PATH = ROOT / "resources" / "e2e_pipeline.job.yml"


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_orchestration_dag_is_acyclic() -> None:
    dag = load(CONTRACT_PATH)["dag"]
    incoming = {
        task: len(spec["depends_on"])
        for task, spec in dag.items()
    }
    outgoing = {task: [] for task in dag}

    for task, spec in dag.items():
        for dependency in spec["depends_on"]:
            assert dependency in dag
            outgoing[dependency].append(task)

    ready = [
        task
        for task, count in incoming.items()
        if count == 0
    ]
    visited = 0

    while ready:
        task = ready.pop()
        visited += 1

        for child in outgoing[task]:
            incoming[child] -= 1
            if incoming[child] == 0:
                ready.append(child)

    assert visited == len(dag)


def test_expected_stage_order_is_declared() -> None:
    dag = load(CONTRACT_PATH)["dag"]

    assert dag["silver_transformation"]["depends_on"] == [
        "bronze_ingestion"
    ]
    assert dag["observability_metrics"]["depends_on"] == [
        "silver_transformation"
    ]
    assert dag["gold_bi"]["depends_on"] == [
        "observability_metrics"
    ]
    assert dag["gold_ml"]["depends_on"] == [
        "observability_metrics"
    ]
    assert set(dag["governance"]["depends_on"]) == {
        "gold_bi",
        "gold_ml",
    }
    assert dag["e2e_validation"]["depends_on"] == [
        "governance"
    ]


def test_recovery_is_explicitly_rerun_safe() -> None:
    contract = load(CONTRACT_PATH)
    assert contract["recovery"]["rerun_safe"] is True
    assert (
        contract["execution"]["recovery_strategy"]
        == "safe_full_rerun"
    )


def test_orchestrator_has_single_concurrent_run() -> None:
    resource = load(RESOURCE_PATH)
    job = resource["resources"]["jobs"]["e2e_pipeline_job"]
    assert job["max_concurrent_runs"] == 1


def test_all_runtime_tasks_exist() -> None:
    resource = load(RESOURCE_PATH)
    tasks = {
        task["task_key"]: task
        for task in resource["resources"]["jobs"][
            "e2e_pipeline_job"
        ]["tasks"]
    }

    assert set(tasks) == {
        "bronze_ingestion",
        "silver_transformation",
        "observability_metrics",
        "gold_bi",
        "gold_ml",
        "governance",
        "e2e_validation",
    }


def test_ml_task_uses_ml_environment() -> None:
    resource = load(RESOURCE_PATH)
    tasks = {
        task["task_key"]: task
        for task in resource["resources"]["jobs"][
            "e2e_pipeline_job"
        ]["tasks"]
    }
    assert tasks["gold_ml"]["environment_key"] == "ml"


def test_non_ml_tasks_use_default_environment() -> None:
    resource = load(RESOURCE_PATH)
    tasks = {
        task["task_key"]: task
        for task in resource["resources"]["jobs"][
            "e2e_pipeline_job"
        ]["tasks"]
    }

    for key in {
        "bronze_ingestion",
        "silver_transformation",
        "observability_metrics",
        "gold_bi",
        "governance",
        "e2e_validation",
    }:
        assert tasks[key]["environment_key"] == "default"


def test_stage_retries_are_bounded() -> None:
    resource = load(RESOURCE_PATH)
    tasks = resource["resources"]["jobs"][
        "e2e_pipeline_job"
    ]["tasks"]

    retryable = [
        task
        for task in tasks
        if task["task_key"] != "e2e_validation"
    ]

    assert all(task["max_retries"] == 1 for task in retryable)
    assert all(
        task["min_retry_interval_millis"] == 30000
        for task in retryable
    )
    assert all(
        task["retry_on_timeout"] is True
        for task in retryable
    )


def test_e2e_validation_is_terminal_task() -> None:
    resource = load(RESOURCE_PATH)
    tasks = resource["resources"]["jobs"][
        "e2e_pipeline_job"
    ]["tasks"]

    depended_on = {
        dependency["task_key"]
        for task in tasks
        for dependency in task.get("depends_on", [])
    }

    assert "e2e_validation" not in depended_on


def test_validation_contract_requires_cross_layer_integrity() -> None:
    validation = load(CONTRACT_PATH)["validation"]

    assert validation["require_row_accounting"] is True
    assert validation["require_observability_health"] == "HEALTHY"
    assert validation["require_governance_status"] == "PASS"
    assert validation["require_gold_fact_matches_silver"] is True
    assert validation["require_ml_feature_segment_match"] is True
    assert (
        validation["require_cross_layer_signature_alignment"]
        is True
    )
