from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
CONTRACT_PATH = ROOT / "config" / "ml_contract.yaml"


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_ml_datasets_are_declared() -> None:
    contract = load_contract()
    assert set(contract["datasets"]) == {
        "customer_features",
        "product_metrics",
        "customer_segments",
    }


def test_kmeans_features_match_reference_design() -> None:
    features = load_contract()["model"]["feature_columns"]
    assert features == [
        "total_orders",
        "total_revenue",
        "avg_ticket",
        "recency_days",
        "distinct_products",
    ]


def test_direct_customer_pii_is_forbidden() -> None:
    contract = load_contract()
    forbidden = set(contract["privacy"]["forbidden_columns"])
    assert {"email", "nome_cliente", "customer_name"} <= forbidden

    published = {
        column
        for dataset in contract["datasets"].values()
        for column in dataset["columns"]
    }
    assert forbidden.isdisjoint(published)


def test_customer_key_is_pseudonymous_identifier() -> None:
    contract = load_contract()
    assert contract["privacy"]["allow_pseudonymous_customer_key"] is True
    assert "customer_key" in contract["datasets"]["customer_features"]["columns"]
    assert "customer_key" in contract["datasets"]["customer_segments"]["columns"]


def test_model_is_reproducibly_configured() -> None:
    model = load_contract()["model"]
    assert model["algorithm"] == "KMeans"
    assert int(model["n_clusters"]) > 1
    assert int(model["random_state"]) == 42
    assert int(model["n_init"]) >= 10
    assert model["scaler"] == "StandardScaler"


def test_mlflow_tracking_is_enabled_without_registry_requirement() -> None:
    mlflow = load_contract()["mlflow"]
    assert mlflow["log_model"] is True
    assert mlflow["register_model"] is False


def test_environment_version_supports_ml_stack() -> None:
    assert load_contract()["runtime"]["serverless_environment_version"] == "5"


def test_quality_gates_are_enabled() -> None:
    quality = load_contract()["quality"]
    assert all(bool(value) for value in quality.values())


def test_segments_publish_last() -> None:
    assert load_contract()["publication"]["publish_segments_last"] is True
