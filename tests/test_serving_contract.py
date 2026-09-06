from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
CONTRACT_PATH = ROOT / "config" / "serving_contract.yaml"


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_power_bi_uses_star_schema_as_primary_contract() -> None:
    contract = load_contract()
    assert contract["power_bi"]["primary_objects"] == [
        "dim_date",
        "dim_customer",
        "dim_product",
        "fact_sales",
    ]


def test_import_is_recommended_for_current_project() -> None:
    assert (
        load_contract()["power_bi"]["recommended_connectivity_mode"]
        == "Import"
    )


def test_development_authentication_is_oauth() -> None:
    assert (
        load_contract()["power_bi"]["development_authentication"]
        == "OAuth"
    )


def test_production_authentication_is_service_principal_oauth() -> None:
    assert (
        load_contract()["power_bi"]["production_authentication"]
        == "ServicePrincipalOAuth"
    )


def test_relationships_are_one_to_many_into_fact() -> None:
    relationships = load_contract()["relationships"]

    assert len(relationships) == 3

    for relationship in relationships:
        assert relationship["to_table"] == "fact_sales"
        assert relationship["cardinality"] == "one_to_many"
        assert relationship["cross_filter"] == "single"


def test_email_is_forbidden() -> None:
    contract = load_contract()
    assert "email" in contract["privacy"]["forbidden_columns"]
    assert (
        contract["privacy"]["require_no_direct_email_in_serving_views"]
        is True
    )


def test_sql_validation_gates_are_enabled() -> None:
    validation = load_contract()["validation"]
    assert all(bool(value) for value in validation.values())


def test_optional_views_are_declared() -> None:
    assert set(load_contract()["power_bi"]["optional_views"]) == {
        "v_sales_enriched",
        "v_sales_monthly",
    }
