from pathlib import Path
import yaml

CONTRACT_PATH = Path(__file__).parents[1] / "config" / "data_contract.yaml"

def load_contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))

def test_contract_has_unique_canonical_and_alias_names() -> None:
    contract = load_contract()
    seen: set[str] = set()
    for canonical, spec in contract["columns"].items():
        for name in [canonical, *spec.get("aliases", [])]:
            assert name not in seen, f"Column/alias declared more than once: {name}"
            seen.add(name)

def test_required_identity_columns_are_required() -> None:
    contract = load_contract()
    for name in ["id_transacao", "id_cliente_final", "data_pedido"]:
        assert contract["columns"][name]["required"] is True

def test_quality_sla_is_percentage() -> None:
    threshold = float(load_contract()["quality"]["minimum_valid_row_pct"])
    assert 0.0 < threshold <= 100.0

def test_expected_quality_rules_exist() -> None:
    rules = load_contract()["quality_rules"]
    assert set(rules) == {f"Q{i:03d}" for i in range(1, 10)}
