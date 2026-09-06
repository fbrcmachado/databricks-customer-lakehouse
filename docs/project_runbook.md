# Project Runbook

## Normal execution

```powershell
databricks bundle run e2e_pipeline_job -p customer-lakehouse
```

A healthy no-change run should result in upstream stages skipping unchanged
state and the terminal E2E validation returning `VALIDATED`.

## Pre-release validation

```powershell
.\scripts\final_validation.ps1
```

To include deployment and a real E2E execution:

```powershell
.\scripts\final_validation.ps1 -Deploy -RunE2E
```

## Failure handling

Do not manually delete Delta tables or state tables as the first recovery
action.

1. identify the failed task and preserve its output;
2. determine whether the failure is transient or deterministic;
3. correct the source, contract, code or configuration;
4. rerun the complete E2E workflow;
5. confirm the terminal E2E validator is healthy.

The pipeline was explicitly tested with a Bronze contract failure followed by
a safe full rerun.

## Power BI

The development PBIX uses the Databricks SQL Warehouse serving the `gold_bi`
star schema. Current DEV authentication uses a PAT because the project runs in
Databricks Free Edition.

Do not commit PBIX files. Keep semantic logic reproducible through contracts,
DAX and documentation.

## Secrets

Never put PATs, OAuth secrets, client secrets or private keys in source files,
notebooks, YAML contracts or documentation.

## Release evidence

A release candidate is acceptable only when:

- the full pytest suite passes;
- repository integrity passes;
- the bundle validates against the target environment;
- the E2E workflow terminates successfully;
- terminal validation reports `pipeline_status=HEALTHY`;
- governance reports `PASS`;
- `git status --short` is empty.
