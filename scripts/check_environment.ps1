param(
    [string]$Profile = "customer-lakehouse"
)

$ErrorActionPreference = "Stop"

Write-Host "Python:"
python --version

Write-Host "`nGit:"
git --version

Write-Host "`nDatabricks CLI:"
databricks -v

Write-Host "`nDatabricks profile:"
databricks current-user me --profile $Profile | Out-Null
Write-Host "$Profile -> VALID"

Write-Host "`nRepository:"
git status --short --branch

Write-Host "`nEnvironment check completed successfully."