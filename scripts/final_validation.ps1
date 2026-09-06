param(
    [string]$Profile = "customer-lakehouse",
    [switch]$Deploy,
    [switch]$RunE2E
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Name,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "============================================================"
    Write-Host $Name
    Write-Host "============================================================"

    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Name"
    }
}

Invoke-Step "Repository integrity" {
    uv run python scripts/repository_integrity.py
}

Invoke-Step "Full regression suite" {
    uv run pytest -q
}

Invoke-Step "Databricks bundle validation" {
    databricks bundle validate -p $Profile
}

if ($Deploy) {
    Invoke-Step "Databricks bundle deployment" {
        databricks bundle deploy -p $Profile
    }
}

if ($RunE2E) {
    if (-not $Deploy) {
        throw "-RunE2E requires -Deploy so the tested code is deployed first."
    }

    Invoke-Step "End-to-end runtime validation" {
        databricks bundle run e2e_pipeline_job -p $Profile
    }
}

Write-Host ""
Write-Host "============================================================"
Write-Host "FINAL VALIDATION: PASS"
Write-Host "============================================================"
Write-Host "Repository integrity : PASS"
Write-Host "Regression tests      : PASS"
Write-Host "Bundle validation     : PASS"
Write-Host "Deployment            : $($Deploy.IsPresent)"
Write-Host "E2E runtime           : $($RunE2E.IsPresent)"
Write-Host "============================================================"
