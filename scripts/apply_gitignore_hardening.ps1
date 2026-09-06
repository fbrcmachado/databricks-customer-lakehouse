$ErrorActionPreference = "Stop"

$requiredLines = @(
    "*.pbix",
    ".databricks/",
    ".venv/",
    ".pytest_cache/",
    "__pycache__/",
    "*.pyc"
)

$path = ".gitignore"

if (Test-Path $path) {
    $current = Get-Content $path
}
else {
    $current = @()
}

$missing = @()

foreach ($line in $requiredLines) {
    if ($current -notcontains $line) {
        $missing += $line
    }
}

if ($missing.Count -eq 0) {
    Write-Host ".gitignore already contains all Phase 14 hardening entries."
    exit 0
}

Add-Content -Path $path -Value ""
Add-Content -Path $path -Value "# Phase 14 hardening"
Add-Content -Path $path -Value $missing

Write-Host "Added to .gitignore:"
$missing | ForEach-Object { Write-Host "  $_" }
