# Architecture

## Target flow

```text
CSV source files
      |
      v
Unity Catalog Volume
      |
      v
Bronze Delta
      |
      v
Silver Delta --------> Quarantine Delta
      |
      v
Quality + Governance Gate
      |
      v
Gold
  |       |         |
  v       v         v
BI       ML     Observability
  |       |         |
  |     MLflow      |
  +-------+---------+
          |
          v
Databricks SQL
          |
          v
Power BI
```

## Source of truth

Original CSV files uploaded to the Unity Catalog Volume are treated as immutable source data.

## Publication principle

Gold data is not publishable merely because transformation code completed. Contract, quality and governance checks must pass before publication.