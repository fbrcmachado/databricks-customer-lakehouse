from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
OBSERVABILITY_CONTRACT = ROOT / "config" / "observability_contract.yaml"
DATA_CONTRACT = ROOT / "config" / "data_contract.yaml"


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_observability_thresholds_are_valid() -> None:
    contract = load_yaml(OBSERVABILITY_CONTRACT)
    quality = contract["quality"]

    assert int(quality["expected_file_count"]) > 0
    assert 0.0 < float(quality["minimum_valid_row_pct"]) <= 100.0
    assert quality["require_exact_row_accounting"] is True


def test_health_statuses_are_unique() -> None:
    health = load_yaml(OBSERVABILITY_CONTRACT)["health"]
    statuses = {
        health["healthy_status"],
        health["degraded_status"],
        health["critical_status"],
    }
    assert len(statuses) == 3


def test_data_quality_rule_codes_are_well_formed() -> None:
    rules = load_yaml(DATA_CONTRACT)["quality_rules"]
    assert set(rules) == {f"Q{i:03d}" for i in range(1, 10)}
