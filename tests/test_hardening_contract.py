from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
CONTRACT_PATH = ROOT / "config" / "hardening_contract.yaml"


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_ci_is_github_actions() -> None:
    assert load_contract()["ci"]["provider"] == "github_actions"


def test_ci_is_credential_free_by_design() -> None:
    ci_text = (ROOT / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )
    assert "DATABRICKS_TOKEN" not in ci_text
    assert "pytest -q" in ci_text
    assert "repository_integrity.py" in ci_text


def test_locked_dependency_sync_is_required() -> None:
    contract = load_contract()
    assert "locked_dependency_sync" in contract["ci"]["required_gates"]


def test_pbix_is_prohibited_in_git() -> None:
    assert load_contract()["repository"]["prohibit_pbix_in_git"] is True


def test_literal_secrets_are_prohibited() -> None:
    assert (
        load_contract()["repository"]["prohibit_literal_secrets"]
        is True
    )


def test_dev_deployment_is_manual() -> None:
    contract = load_contract()
    assert contract["cd"]["trigger"] == "workflow_dispatch"


def test_dev_bundle_validation_is_required() -> None:
    contract = load_contract()
    assert contract["cd"]["validate_bundle"] is True
    assert contract["cd"]["deploy_bundle"] is True


def test_current_dev_authentication_is_explicitly_non_production() -> None:
    contract = load_contract()
    assert (
        contract["cd"]["current_authentication"]
        == "personal_access_token_github_secret"
    )
    assert contract["production_target"]["implemented"] is False


def test_production_requires_workload_identity_federation() -> None:
    contract = load_contract()
    assert (
        contract["production_target"]["required_authentication"]
        == "github_oidc_workload_identity_federation"
    )
    assert contract["production_target"]["require_service_principal"] is True


def test_production_requires_environment_approval() -> None:
    assert (
        load_contract()["production_target"][
            "require_environment_approval"
        ]
        is True
    )


def test_release_gate_requires_runtime_health_and_governance() -> None:
    gate = load_contract()["release_gate"]
    assert gate["require_e2e_validation"] is True
    assert gate["require_pipeline_health"] == "HEALTHY"
    assert gate["require_governance_status"] == "PASS"


def test_enterprise_gaps_are_not_hidden() -> None:
    gaps = set(load_contract()["known_enterprise_gaps"])
    assert "no_separate_prod_workspace_or_catalog_yet" in gaps
    assert "current_dev_authentication_is_personal" in gaps
    assert "no_disaster_recovery_test" in gaps
    assert "no_load_or_cost_benchmark" in gaps
