# Databricks Customer Lakehouse

A production-inspired educational data platform built on **Databricks Free Edition**.

The project ports the architectural principles of the local Customer Lakehouse implementation to Databricks-native capabilities:

- Unity Catalog
- Delta Lake
- serverless compute
- Medallion Architecture
- incremental ingestion
- data contracts
- quarantine and data quality
- Governance as Code
- observability
- MLflow
- Databricks SQL
- Power BI

## Current status

**Phase 3 â€” Repository bootstrap**

No Databricks data objects are created during this phase.

## Planned logical structure

```text
customer_lakehouse
â”œâ”€â”€ landing
â”œâ”€â”€ bronze
â”œâ”€â”€ silver
â”œâ”€â”€ quarantine
â”œâ”€â”€ gold_bi
â”œâ”€â”€ gold_ml
â”œâ”€â”€ observability
â””â”€â”€ governance
```

## Engineering principles

- Technology follows the problem.
- Source data is immutable.
- Data contracts are explicit.
- Incremental processing must be idempotent.
- Invalid records are quarantined instead of silently discarded.
- Governable policies should be executable.
- Candidate outputs are validated before publication.
- Last Known Good data is preserved when publication fails.
- Test-not-run is not test-approved.