from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
CONTRACT_PATH = ROOT / "config" / "gold_contract.yaml"


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_gold_has_expected_tables() -> None:
    contract = load_contract()
    assert set(contract["tables"]) == {
        "dim_date",
        "dim_customer",
        "dim_product",
        "fact_sales",
    }


def test_email_is_forbidden_from_gold_bi() -> None:
    contract = load_contract()
    assert "email" in contract["privacy"]["forbidden_columns"]

    all_columns = {
        column
        for table in contract["tables"].values()
        for column in table["columns"]
    }
    assert "email" not in all_columns


def test_fact_has_required_foreign_keys() -> None:
    columns = set(load_contract()["tables"]["fact_sales"]["columns"])
    assert {"date_key", "customer_key", "product_key"} <= columns


def test_fact_transaction_id_is_declared() -> None:
    columns = load_contract()["tables"]["fact_sales"]["columns"]
    assert columns[0] == "transaction_id"


def test_quality_gates_are_enabled() -> None:
    quality = load_contract()["quality"]
    assert all(bool(value) for value in quality.values())


def test_fact_is_published_last() -> None:
    assert load_contract()["publication"]["publish_fact_last"] is True
