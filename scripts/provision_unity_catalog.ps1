param(
    [string]$Profile = "customer-lakehouse",
    [string]$Catalog = "customer_lakehouse"
)

$ErrorActionPreference = "Stop"

$Schemas = [ordered]@{
    "landing"       = "Landing zone for immutable source files"
    "bronze"        = "Raw ingestion layer with lineage metadata"
    "silver"        = "Canonical, typed and validated business data"
    "quarantine"    = "Rejected rows and data quality evidence"
    "gold_bi"       = "Curated analytical serving layer for BI"
    "gold_ml"       = "Feature and machine learning result layer"
    "observability" = "Operational metadata, pipeline history and quality metrics"
    "governance"    = "Governance metadata, controls, glossary and lineage declarations"
}

$Volume = "source_files"

function Invoke-Databricks {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    # Windows PowerShell 5.x can promote native stderr into NativeCommandError
    # when ErrorActionPreference = Stop. We handle native exit codes explicitly.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        $output = & databricks @Arguments --profile $Profile -o json 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }

    $outputText = ($output | ForEach-Object { $_.ToString() }) -join "`n"

    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "Databricks CLI failed:`n  databricks $($Arguments -join ' ')`n$outputText"
    }

    return @{
        ExitCode = $exitCode
        Output   = $outputText
    }
}

function Test-CatalogExists {
    $result = Invoke-Databricks -Arguments @("catalogs", "get", $Catalog) -AllowFailure
    return ($result.ExitCode -eq 0)
}

function Test-SchemaExists {
    param([string]$Schema)

    $result = Invoke-Databricks -Arguments @(
        "schemas", "get", "$Catalog.$Schema"
    ) -AllowFailure

    return ($result.ExitCode -eq 0)
}

function Test-VolumeExists {
    $result = Invoke-Databricks -Arguments @(
        "volumes", "read", "$Catalog.landing.$Volume"
    ) -AllowFailure

    return ($result.ExitCode -eq 0)
}

function Get-SqlWarehouseId {
    $result = Invoke-Databricks -Arguments @("warehouses", "list")
    $json = $result.Output | ConvertFrom-Json

    $warehouses = @()

    if ($null -ne $json.warehouses) {
        $warehouses = @($json.warehouses)
    }
    elseif ($json -is [System.Array]) {
        $warehouses = @($json)
    }
    else {
        $warehouses = @($json)
    }

    if ($warehouses.Count -eq 0) {
        throw "No SQL Warehouse is accessible. Databricks Free Edition normally provides a Starter Warehouse."
    }

    $preferred = $warehouses |
        Where-Object { $_.name -eq "Serverless Starter Warehouse" } |
        Select-Object -First 1

    if ($null -eq $preferred) {
        $preferred = $warehouses |
            Where-Object { $_.enable_serverless_compute -eq $true } |
            Select-Object -First 1
    }

    if ($null -eq $preferred) {
        $preferred = $warehouses | Select-Object -First 1
    }

    if (-not $preferred.id) {
        throw "Unable to resolve a SQL Warehouse ID."
    }

    Write-Host "[INFO] SQL Warehouse selected: $($preferred.name) [$($preferred.id)]"
    return $preferred.id
}

function Invoke-SqlStatement {
    param(
        [Parameter(Mandatory = $true)][string]$WarehouseId,
        [Parameter(Mandatory = $true)][string]$Statement
    )

    $body = @{
        warehouse_id = $WarehouseId
        statement    = $Statement
        wait_timeout = "50s"
        disposition  = "INLINE"
        format       = "JSON_ARRAY"
    } | ConvertTo-Json -Compress

    Write-Host "[SQL] $Statement"

    $submit = Invoke-Databricks -Arguments @(
        "api", "post", "/api/2.0/sql/statements",
        "--json", $body
    )

    $response = $submit.Output | ConvertFrom-Json
    $statementId = $response.statement_id

    if (-not $statementId) {
        throw "Statement Execution API did not return a statement_id."
    }

    $state = $response.status.state

    while ($state -in @("PENDING", "RUNNING")) {
        Start-Sleep -Seconds 2

        $poll = Invoke-Databricks -Arguments @(
            "api", "get", "/api/2.0/sql/statements/$statementId"
        )

        $response = $poll.Output | ConvertFrom-Json
        $state = $response.status.state
    }

    if ($state -ne "SUCCEEDED") {
        $errorMessage = $response.status.error.message
        if (-not $errorMessage) {
            $errorMessage = ($response | ConvertTo-Json -Depth 10)
        }

        throw "SQL statement failed with state '$state': $errorMessage"
    }

    Write-Host "[SQL PASS] Statement $statementId"
}

Write-Host ""
Write-Host "Databricks Customer Lakehouse - Unity Catalog Bootstrap"
Write-Host "======================================================="
Write-Host ""

# Gate 1: authentication
Write-Host "[GATE] Validating Databricks authentication..."
Invoke-Databricks -Arguments @("current-user", "me") | Out-Null
Write-Host "[PASS] Databricks profile '$Profile' is valid."

# Gate 2: catalog
Write-Host ""
Write-Host "[STEP] Catalog: $Catalog"

if (Test-CatalogExists) {
    Write-Host "[EXISTS] $Catalog"
}
else {
    Write-Host "[CREATE] $Catalog using Databricks SQL Default Storage semantics"

    $warehouseId = Get-SqlWarehouseId

    $catalogSql = @"
CREATE CATALOG IF NOT EXISTS $Catalog
COMMENT 'Educational Customer Lakehouse built on Databricks Free Edition'
"@

    Invoke-SqlStatement -WarehouseId $warehouseId -Statement $catalogSql
}

if (-not (Test-CatalogExists)) {
    throw "Catalog '$Catalog' could not be validated after provisioning."
}

Write-Host "[VALIDATED] Catalog $Catalog"

# Gate 3: schemas
Write-Host ""
Write-Host "[STEP] Schemas"

foreach ($schema in $Schemas.Keys) {
    $fullName = "$Catalog.$schema"

    if (Test-SchemaExists -Schema $schema) {
        Write-Host "[EXISTS] $fullName"
        continue
    }

    Write-Host "[CREATE] $fullName"

    Invoke-Databricks -Arguments @(
        "schemas", "create", $schema, $Catalog,
        "--comment", $Schemas[$schema]
    ) | Out-Null

    if (-not (Test-SchemaExists -Schema $schema)) {
        throw "Schema '$fullName' could not be validated after provisioning."
    }

    Write-Host "[CREATED] $fullName"
}

# Gate 4: managed volume
$volumeFullName = "$Catalog.landing.$Volume"

Write-Host ""
Write-Host "[STEP] Managed Volume: $volumeFullName"

if (Test-VolumeExists) {
    Write-Host "[EXISTS] $volumeFullName"
}
else {
    Write-Host "[CREATE] $volumeFullName"

    Invoke-Databricks -Arguments @(
        "volumes", "create",
        $Catalog,
        "landing",
        $Volume,
        "MANAGED",
        "--comment", "Immutable landing volume for original customer sales CSV files"
    ) | Out-Null

    if (-not (Test-VolumeExists)) {
        throw "Volume '$volumeFullName' could not be validated after provisioning."
    }

    Write-Host "[CREATED] $volumeFullName"
}

# Final verification
Write-Host ""
Write-Host "[VERIFY] Reading provisioned objects..."

Invoke-Databricks -Arguments @("catalogs", "get", $Catalog) | Out-Null
Invoke-Databricks -Arguments @("schemas", "list", $Catalog) | Out-Null
Invoke-Databricks -Arguments @("volumes", "read", $volumeFullName) | Out-Null

Write-Host ""
Write-Host "Provisioning complete."
Write-Host "----------------------"
Write-Host "Catalog:  $Catalog"
Write-Host "Schemas:  $($Schemas.Count) project schemas"
Write-Host "Volume:   $volumeFullName"
Write-Host ""
Write-Host "No tables, jobs, or pipelines were created."
Write-Host "Evidence level: ENVIRONMENT VALIDATED"
