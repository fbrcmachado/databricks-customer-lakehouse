from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]


def load(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def test_all_asset_ids_are_unique() -> None:
    assets = load("governance/catalog/assets.yaml")["assets"]
    ids = [asset["asset_id"] for asset in assets]
    assert len(ids) == len(set(ids))


def test_all_assets_have_governance_metadata() -> None:
    assets = load("governance/catalog/assets.yaml")["assets"]
    required = {
        "asset_id",
        "object_name",
        "object_type",
        "domain",
        "owner_role",
        "steward_role",
        "classification",
        "description",
    }
    assert all(required <= set(asset) for asset in assets)


def test_asset_domains_and_roles_exist() -> None:
    assets = load("governance/catalog/assets.yaml")["assets"]
    ownership = load("governance/ownership/domains.yaml")
    domains = set(ownership["domains"])
    roles = set(ownership["roles"])

    for asset in assets:
        assert asset["domain"] in domains
        assert asset["owner_role"] in roles
        assert asset["steward_role"] in roles


def test_asset_classifications_are_valid() -> None:
    assets = load("governance/catalog/assets.yaml")["assets"]
    valid = set(
        load("governance/classification/data_classification.yaml")["levels"]
    )
    assert all(asset["classification"] in valid for asset in assets)


def test_column_classification_assets_exist() -> None:
    asset_ids = {
        asset["asset_id"]
        for asset in load("governance/catalog/assets.yaml")["assets"]
    }
    entries = load(
        "governance/classification/data_classification.yaml"
    )["column_classifications"]
    assert all(entry["asset_id"] in asset_ids for entry in entries)


def test_lineage_references_known_assets() -> None:
    asset_ids = {
        asset["asset_id"]
        for asset in load("governance/catalog/assets.yaml")["assets"]
    }
    edges = load("governance/lineage/lineage.yaml")["edges"]
    assert all(
        edge["from"] in asset_ids and edge["to"] in asset_ids
        for edge in edges
    )


def test_lineage_is_acyclic() -> None:
    assets = load("governance/catalog/assets.yaml")["assets"]
    edges = load("governance/lineage/lineage.yaml")["edges"]

    nodes = {asset["asset_id"] for asset in assets}
    incoming = {node: 0 for node in nodes}
    outgoing = {node: [] for node in nodes}

    for edge in edges:
        outgoing[edge["from"]].append(edge["to"])
        incoming[edge["to"]] += 1

    ready = [node for node, count in incoming.items() if count == 0]
    visited = 0

    while ready:
        node = ready.pop()
        visited += 1
        for child in outgoing[node]:
            incoming[child] -= 1
            if incoming[child] == 0:
                ready.append(child)

    assert visited == len(nodes)


def test_glossary_references_known_assets_and_domains() -> None:
    asset_ids = {
        asset["asset_id"]
        for asset in load("governance/catalog/assets.yaml")["assets"]
    }
    domains = set(load("governance/ownership/domains.yaml")["domains"])
    terms = load("governance/glossary/business_terms.yaml")["terms"]

    for term in terms:
        assert term["domain"] in domains
        assert set(term["related_assets"]) <= asset_ids


def test_retention_is_report_only_and_non_destructive() -> None:
    retention = load("governance/policies/retention.yaml")
    assert retention["enforcement_mode"] == "report_only"
    assert retention["destructive_enforcement_enabled"] is False
    assert all(int(rule["retention_days"]) > 0 for rule in retention["rules"])


def test_pii_policy_blocks_email_from_gold_bi() -> None:
    rules = load("governance/policies/pii.yaml")["rules"]
    gold_bi = next(rule for rule in rules if rule["asset_prefix"] == "gold_bi.")
    assert "email" in gold_bi["forbidden_columns"]


def test_pii_policy_blocks_direct_identity_from_gold_ml() -> None:
    rules = load("governance/policies/pii.yaml")["rules"]
    gold_ml = next(rule for rule in rules if rule["asset_prefix"] == "gold_ml.")
    forbidden = set(gold_ml["forbidden_columns"])
    assert {"email", "customer_name", "nome_cliente"} <= forbidden


def test_quality_sla_matches_pipeline_threshold() -> None:
    policy = load("governance/policies/quality_sla.yaml")
    contract = load("config/governance_contract.yaml")
    assert float(policy["minimum_valid_row_pct"]) == float(
        contract["evidence"]["minimum_valid_row_pct"]
    )


def test_governance_runtime_is_blocking() -> None:
    contract = load("config/governance_contract.yaml")
    assert contract["runtime"]["fail_on_blocking_control"] is True


def test_uc_comments_are_enabled() -> None:
    contract = load("config/governance_contract.yaml")
    assert contract["runtime"]["apply_uc_comments"] is True
