param(
    [string]$Profile = "customer-lakehouse",
    [string]$SourceDir = ".\source",
    [string]$Catalog = "customer_lakehouse",
    [string]$Schema = "landing",
    [string]$Volume = "source_files",
    [int]$ExpectedFileCount = 50
)

$ErrorActionPreference = "Stop"

$RemoteRoot = "dbfs:/Volumes/$Catalog/$Schema/$Volume"
$RemoteMetaDir = "$RemoteRoot/_meta"
$RemoteManifest = "$RemoteMetaDir/source_manifest.json"
$LocalStateDir = ".\.databricks"
$LocalManifest = Join-Path $LocalStateDir "source_manifest.json"

function Invoke-Databricks {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )

    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        $output = & databricks @Arguments --profile $Profile 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }

    $text = ($output | ForEach-Object { $_.ToString() }) -join "`n"

    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "Databricks CLI failed:`n  databricks $($Arguments -join ' ')`n$text"
    }

    return @{
        ExitCode = $exitCode
        Output   = $text
    }
}

function Get-RemoteFileHash {
    param(
        [Parameter(Mandatory = $true)][string]$RemotePath
    )

    $tempPath = Join-Path `
        ([System.IO.Path]::GetTempPath()) `
        ("dbx_" + [Guid]::NewGuid().ToString("N") + ".tmp")

    try {
        $result = Invoke-Databricks -Arguments @(
            "fs", "cp", $RemotePath, $tempPath, "--overwrite"
        ) -AllowFailure

        if ($result.ExitCode -ne 0) {
            return $null
        }

        return (Get-FileHash -Algorithm SHA256 -Path $tempPath).Hash.ToLowerInvariant()
    }
    finally {
        Remove-Item $tempPath -Force -ErrorAction SilentlyContinue
    }
}

Write-Host ""
Write-Host "Databricks Customer Lakehouse - Source Seed"
Write-Host "============================================"
Write-Host ""

# Gate 1: authentication
Write-Host "[GATE] Validating Databricks authentication..."
Invoke-Databricks -Arguments @("current-user", "me", "-o", "json") | Out-Null
Write-Host "[PASS] Profile '$Profile' is valid."

# Gate 2: local source directory
$resolvedSource = Resolve-Path $SourceDir -ErrorAction SilentlyContinue
if (-not $resolvedSource) {
    throw "Source directory '$SourceDir' does not exist."
}

$sourcePath = $resolvedSource.Path
$csvFiles = @(
    Get-ChildItem -Path $sourcePath -File -Filter "*.csv" |
    Sort-Object Name
)

if ($csvFiles.Count -ne $ExpectedFileCount) {
    throw "Expected exactly $ExpectedFileCount CSV files in '$sourcePath', found $($csvFiles.Count)."
}

Write-Host "[PASS] Found exactly $($csvFiles.Count) CSV source files."

# Gate 3: target managed volume
Write-Host "[GATE] Validating target volume..."
Invoke-Databricks -Arguments @(
    "volumes", "read", "$Catalog.$Schema.$Volume", "-o", "json"
) | Out-Null
Write-Host "[PASS] Volume '$Catalog.$Schema.$Volume' exists."

# Build deterministic local manifest
if (-not (Test-Path $LocalStateDir)) {
    New-Item -ItemType Directory -Path $LocalStateDir -Force | Out-Null
}

$manifestFiles = foreach ($file in $csvFiles) {
    $hash = (Get-FileHash -Algorithm SHA256 -Path $file.FullName).Hash.ToLowerInvariant()

    [ordered]@{
        file_name = $file.Name
        size_bytes = $file.Length
        sha256 = $hash
    }
}

$manifest = [ordered]@{
    catalog = $Catalog
    schema = $Schema
    volume = $Volume
    expected_file_count = $ExpectedFileCount
    files = @($manifestFiles)
}

$manifestJson = $manifest | ConvertTo-Json -Depth 5

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
    (Join-Path (Get-Location) $LocalManifest),
    $manifestJson,
    $utf8NoBom
)

Write-Host "[PASS] Local SHA-256 manifest generated."

# Upload / validate every immutable source file
$uploaded = 0
$skipped = 0
$verified = 0

Write-Host ""
Write-Host "[STEP] Uploading and validating immutable source files"

foreach ($file in $csvFiles) {
    $localHash = (Get-FileHash -Algorithm SHA256 -Path $file.FullName).Hash.ToLowerInvariant()
    $remotePath = "$RemoteRoot/$($file.Name)"
    $remoteHash = Get-RemoteFileHash -RemotePath $remotePath

    if ($null -ne $remoteHash) {
        if ($remoteHash -ne $localHash) {
            throw "IMMUTABILITY VIOLATION: '$remotePath' already exists with a different SHA-256. The script will not overwrite Landing source data."
        }

        Write-Host "[SKIP] $($file.Name) already exists and hash matches."
        $skipped++
        $verified++
        continue
    }

    Write-Host "[UPLOAD] $($file.Name)"

    Invoke-Databricks -Arguments @(
        "fs", "cp", $file.FullName, $remotePath
    ) | Out-Null

    $postUploadHash = Get-RemoteFileHash -RemotePath $remotePath

    if ($postUploadHash -ne $localHash) {
        throw "POST-UPLOAD VALIDATION FAILED: '$($file.Name)' SHA-256 does not match the local source."
    }

    Write-Host "[VERIFIED] $($file.Name)"
    $uploaded++
    $verified++
}

# Publish deterministic source manifest as metadata, not source data
Write-Host ""
Write-Host "[STEP] Publishing source manifest"

Invoke-Databricks -Arguments @(
    "fs", "mkdir", $RemoteMetaDir
) | Out-Null

$localManifestHash = (
    Get-FileHash -Algorithm SHA256 -Path $LocalManifest
).Hash.ToLowerInvariant()

$remoteManifestHash = Get-RemoteFileHash -RemotePath $RemoteManifest

if ($null -ne $remoteManifestHash -and $remoteManifestHash -ne $localManifestHash) {
    throw "MANIFEST CONFLICT: remote source manifest differs from the current local source set."
}

if ($null -eq $remoteManifestHash) {
    Invoke-Databricks -Arguments @(
        "fs", "cp", (Resolve-Path $LocalManifest).Path, $RemoteManifest
    ) | Out-Null
    Write-Host "[CREATED] $RemoteManifest"
}
else {
    Write-Host "[SKIP] Remote source manifest already matches."
}

# Final validation of all source hashes
Write-Host ""
Write-Host "[VERIFY] Final SHA-256 validation of all source files"

foreach ($entry in $manifestFiles) {
    $remotePath = "$RemoteRoot/$($entry.file_name)"
    $remoteHash = Get-RemoteFileHash -RemotePath $remotePath

    if ($remoteHash -ne $entry.sha256) {
        throw "FINAL VALIDATION FAILED: '$($entry.file_name)' does not match the source manifest."
    }
}

Write-Host ""
Write-Host "Source seed complete."
Write-Host "---------------------"
Write-Host "Source directory: $sourcePath"
Write-Host "Remote volume:    $RemoteRoot"
Write-Host "CSV files:        $($csvFiles.Count)"
Write-Host "Uploaded:         $uploaded"
Write-Host "Skipped:          $skipped"
Write-Host "Verified:         $verified"
Write-Host ""
Write-Host "Landing immutability policy: ENFORCED"
Write-Host "Evidence level: ENVIRONMENT VALIDATED"
