param(
    [string]$Profile = "customer-lakehouse",
    [string]$WarehouseName = "Serverless Starter Warehouse",
    [string]$Catalog = "customer_lakehouse"
)

$ErrorActionPreference = "Stop"

function Invoke-DatabricksJson {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & databricks @Arguments 2>&1

    if ($LASTEXITCODE -ne 0) {
        throw "Databricks CLI command failed:`n$($output | Out-String)"
    }

    $text = $output | Out-String

    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    return $text | ConvertFrom-Json
}

function Invoke-SqlStatement {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql,

        [Parameter(Mandatory = $true)]
        [string]$Description,

        [Parameter(Mandatory = $true)]
        [string]$WarehouseId
    )

    Write-Host ""
    Write-Host "Executing: $Description"

    $payload = @{
        warehouse_id = $WarehouseId
        catalog = $Catalog
        schema = "gold_bi"
        statement = $Sql
        wait_timeout = "50s"
        on_wait_timeout = "CONTINUE"
        disposition = "INLINE"
        format = "JSON_ARRAY"
    } | ConvertTo-Json -Depth 20

    $tempFile = Join-Path $env:TEMP (
        "databricks-sql-" + [guid]::NewGuid().ToString("N") + ".json"
    )

    try {
        [System.IO.File]::WriteAllText(
            $tempFile,
            $payload,
            [System.Text.UTF8Encoding]::new($false)
        )

        $response = Invoke-DatabricksJson -Arguments @(
            "api",
            "post",
            "/api/2.0/sql/statements",
            "-p",
            $Profile,
            "--json",
            "@$tempFile",
            "-o",
            "json"
        )

        while (
            $null -ne $response.status -and
            $response.status.state -in @("PENDING", "RUNNING")
        ) {
            Start-Sleep -Seconds 2

            $response = Invoke-DatabricksJson -Arguments @(
                "api",
                "get",
                "/api/2.0/sql/statements/$($response.statement_id)",
                "-p",
                $Profile,
                "-o",
                "json"
            )
        }

        if ($response.status.state -ne "SUCCEEDED") {
            $errorMessage = $response.status.error.message

            if ([string]::IsNullOrWhiteSpace($errorMessage)) {
                $errorMessage = $response | ConvertTo-Json -Depth 20
            }

            throw (
                "SQL statement did not succeed. State=$($response.status.state)`n" +
                $errorMessage
            )
        }

        Write-Host "SUCCEEDED"

        if (
            $null -ne $response.result -and
            $null -ne $response.result.data_array
        ) {
            Write-Host (
                $response.result.data_array |
                ConvertTo-Json -Depth 20
            )
        }

        return $response
    }
    finally {
        if (Test-Path $tempFile) {
            Remove-Item $tempFile -Force
        }
    }
}

function Invoke-SqlFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [string]$WarehouseId
    )

    if (-not (Test-Path $Path)) {
        throw "SQL file not found: $Path"
    }

    $raw = Get-Content $Path -Raw

    $statements = [regex]::Split(
        $raw,
        "(?m)^\s*-- COMMAND ----------\s*$"
    ) |
        ForEach-Object { $_.Trim() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    $index = 0

    foreach ($statement in $statements) {
        $index += 1

        Invoke-SqlStatement `
            -Sql $statement `
            -Description "$Label [$index/$($statements.Count)]" `
            -WarehouseId $WarehouseId | Out-Null
    }
}

Write-Host "Discovering SQL warehouse '$WarehouseName'..."

$warehouseList = Invoke-DatabricksJson -Arguments @(
    "warehouses",
    "list",
    "-p",
    $Profile,
    "-o",
    "json"
)

if ($null -ne $warehouseList.warehouses) {
    $warehouses = @($warehouseList.warehouses)
}
else {
    $warehouses = @($warehouseList)
}

$warehouse = $warehouses |
    Where-Object { $_.name -eq $WarehouseName } |
    Select-Object -First 1

if ($null -eq $warehouse) {
    $available = (
        $warehouses |
        ForEach-Object { $_.name }
    ) -join ", "

    throw (
        "SQL warehouse '$WarehouseName' not found. " +
        "Available warehouses: $available"
    )
}

Write-Host "Warehouse ID: $($warehouse.id)"
Write-Host "Warehouse state: $($warehouse.state)"

if ($warehouse.state -eq "STOPPED") {
    Write-Host "Starting SQL warehouse..."

    & databricks warehouses start $warehouse.id -p $Profile | Out-Null

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to start SQL warehouse $($warehouse.id)."
    }
}

$warehouse = Invoke-DatabricksJson -Arguments @(
    "warehouses",
    "get",
    $warehouse.id,
    "-p",
    $Profile,
    "-o",
    "json"
)

$serverHostname = $warehouse.odbc_params.hostname
$httpPath = $warehouse.odbc_params.path

if (
    [string]::IsNullOrWhiteSpace($serverHostname) -or
    [string]::IsNullOrWhiteSpace($httpPath)
) {
    throw "Warehouse connection details are unavailable."
}

$repoRoot = Split-Path $PSScriptRoot -Parent

Invoke-SqlFile `
    -Path (Join-Path $repoRoot "sql/01_create_powerbi_serving_views.sql") `
    -Label "Create Power BI serving views" `
    -WarehouseId $warehouse.id

Invoke-SqlFile `
    -Path (Join-Path $repoRoot "sql/02_validate_powerbi_serving.sql") `
    -Label "Validate SQL serving layer" `
    -WarehouseId $warehouse.id

Write-Host ""
Write-Host "============================================================"
Write-Host "SQL WAREHOUSE + POWER BI SERVING: VALIDATED"
Write-Host "============================================================"
Write-Host "Warehouse Name : $($warehouse.name)"
Write-Host "Warehouse ID   : $($warehouse.id)"
Write-Host "Server Hostname: $serverHostname"
Write-Host "HTTP Path      : $httpPath"
Write-Host "Catalog        : $Catalog"
Write-Host "Schema         : gold_bi"
Write-Host "Recommended BI : Import"
Write-Host "Dev Auth       : OAuth"
Write-Host ""
Write-Host "Power BI objects:"
Write-Host "  dim_date"
Write-Host "  dim_customer"
Write-Host "  dim_product"
Write-Host "  fact_sales"
Write-Host ""
Write-Host "Optional SQL views:"
Write-Host "  v_sales_enriched"
Write-Host "  v_sales_monthly"
Write-Host "============================================================"
