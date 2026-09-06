# CI/CD

## CI

`.github/workflows/ci.yml` is credential-free and runs on pull requests and
pushes to `main`.

Gates:

1. locked dependency sync with `uv`;
2. Python static compilation;
3. repository integrity validation;
4. the complete pytest suite.

The CI workflow deliberately does not connect to Databricks. Pull-request code
must be testable without exposing workspace credentials.

## DEV deployment

`.github/workflows/deploy-dev.yml` is manual (`workflow_dispatch`). Configure a
GitHub Environment named `dev` with:

```text
Variable:
DATABRICKS_HOST = https://dbc-20c7ddb0-2b23.cloud.databricks.com

Secret:
DATABRICKS_TOKEN = <DEV PAT>
```

The PAT must never be committed to the repository.

The workflow validates and deploys the Declarative Automation Bundle and can
optionally execute `e2e_pipeline_job`.

This is intentionally a DEV-only compromise for the Free Edition workspace.

## Production

Production deployment is not enabled. See
`docs/production_cicd_template.md`.

The target production design is GitHub OIDC workload identity federation using
a dedicated Databricks service principal, short-lived credentials and a GitHub
`prod` environment with approval.

## Supply-chain controls

GitHub's checkout action and the uv setup action are pinned to immutable commit
SHAs in CI. The Databricks setup action and CLI are pinned to explicit release
versions in the DEV deployment workflow.

Revisit those pins periodically rather than silently following `main` or
`latest`.
