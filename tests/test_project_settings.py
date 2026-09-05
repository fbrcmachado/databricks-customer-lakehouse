from customer_lakehouse.settings import CATALOG, SCHEMAS, SOURCE_VOLUME


def test_catalog_name() -> None:
    assert CATALOG == "customer_lakehouse"


def test_required_schemas_are_unique() -> None:
    assert len(SCHEMAS) == len(set(SCHEMAS))


def test_expected_schemas_exist() -> None:
    assert {
        "landing",
        "bronze",
        "silver",
        "quarantine",
        "gold_bi",
        "gold_ml",
        "observability",
        "governance",
    } == set(SCHEMAS)


def test_source_volume_name() -> None:
    assert SOURCE_VOLUME == "source_files"