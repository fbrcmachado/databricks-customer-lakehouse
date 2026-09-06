# Production CI/CD template

This project intentionally does **not** enable production deployment yet.

The current Free Edition development environment uses a personal access token
for manual GitHub Actions deployment. That is acceptable only as a constrained
DEV implementation.

For production, use a dedicated Databricks service principal and GitHub OIDC
workload identity federation. Databricks documents `github-oidc` as the
authentication type for GitHub Actions workload identity federation.

A future production workflow should resemble:

```yaml
name: Deploy PROD

on:
  workflow_dispatch:

permissions:
  id-token: write
  contents: read

concurrency:
  group: databricks-prod
  cancel-in-progress: false

jobs:
  deploy:
    environment: prod
    runs-on: ubuntu-latest

    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}

    steps:
      - uses: actions/checkout@<PINNED_SHA>
        with:
          persist-credentials: false

      - uses: databricks/setup-cli@<PINNED_RELEASE>
        with:
          version: "<PINNED_CLI_VERSION>"

      - run: databricks bundle validate -t prod
      - run: databricks bundle deploy -t prod
      - run: databricks bundle run e2e_pipeline_job -t prod
```

Before enabling this workflow, the repository must also define a real `prod`
bundle target, a separate production catalog/storage boundary, non-personal
ownership and GitHub environment approval.

Do not copy the DEV PAT into a production secret as a shortcut.
