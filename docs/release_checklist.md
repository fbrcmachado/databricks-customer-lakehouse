# Release Checklist

Use this checklist before tagging or presenting a final project release.

- [ ] `uv run pytest -q` passes.
- [ ] `uv run python scripts/repository_integrity.py` passes.
- [ ] `databricks bundle validate -p customer-lakehouse` passes.
- [ ] E2E workflow terminates `SUCCESS`.
- [ ] E2E validator reports `15/15` checks.
- [ ] Pipeline reports `HEALTHY`.
- [ ] Governance reports `PASS` with zero blocking failures.
- [ ] SQL serving validation passes.
- [ ] Power BI refresh has been tested against the SQL Warehouse.
- [ ] No secrets exist in source control.
- [ ] No PBIX is tracked.
- [ ] `git status --short` is empty.
- [ ] Release limitations are documented.
