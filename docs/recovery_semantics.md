# End-to-End Recovery Semantics

Phase 12 introduces one coordinated Databricks workflow for the complete data
product. The default recovery operation is intentionally simple:

```powershell
databricks bundle run e2e_pipeline_job -p customer-lakehouse
```

A full rerun is safe because every persisted stage already implements its own
idempotency or input-signature semantics.

## DAG

```text
Bronze
  |
  v
Silver + Quarantine
  |
  v
Observability
  |
  +------------+
  |            |
  v            v
Gold BI      Gold ML
  |            |
  +-----+------+
        |
        v
Governance
        |
        v
E2E Validation
```

## Recovery behavior

| Failure point | Safe rerun behavior |
|---|---|
| Bronze | Hash manifest prevents duplicate ingestion of unchanged files. |
| Silver | Source snapshot + contract signature controls rebuild/skip. |
| Observability | Observation signature prevents duplicate unchanged snapshots. |
| Gold BI | Silver + Gold contract signature controls rebuild/skip; candidates validate before publish. |
| Gold ML | Silver + ML contract + runtime fingerprint controls retraining/skip. |
| Governance | Governance metadata + UC inventory + product state signature controls reevaluation/skip. |
| E2E validation | Read-only validation can be rerun freely. |

Task-level retry is bounded to one retry with a 30-second minimum delay. The
workflow itself allows only one concurrent run. This limits overlap while still
recovering from transient Serverless or control-plane failures.

The project intentionally does not implement a distributed transaction across
Silver, Gold BI, Gold ML, governance and MLflow. Recovery relies on independent
stage checkpoints, candidate-before-publish behavior and Last Known Good
consumer-facing tables.

A failed deterministic business or governance control may be retried once by
the workflow, but the correct response is to fix the input/contract/policy and
rerun the pipeline. Do not lower an SLA merely to make a run green.

## Decision expiration

Revisit this strategy when one or more of the following becomes true:

- multiple producers can publish the same data product concurrently;
- the workflow requires cross-table atomic cutover;
- partial publication windows become unacceptable to consumers;
- environments move from a single DEV workspace to DEV/PROD promotion;
- recovery objectives require automated repair-run selection rather than safe
  full reruns;
- source volume makes full downstream rebuilds operationally expensive.
