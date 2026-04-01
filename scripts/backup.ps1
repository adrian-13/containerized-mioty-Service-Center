[CmdletBinding()]
param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$OutputRoot = "",
    [string]$DbContainer = "bssci-timescaledb",
    [switch]$NoArchive
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[backup] $Message"
}

function Read-EnvFile {
    param([string]$Path)
    $map = @{}
    if (-not (Test-Path -LiteralPath $Path)) { return $map }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) { continue }
        $idx = $trimmed.IndexOf("=")
        if ($idx -lt 1) { continue }
        $key = $trimmed.Substring(0, $idx).Trim()
        $value = $trimmed.Substring($idx + 1).Trim().Trim("'`"")
        if ($key) { $map[$key] = $value }
    }
    return $map
}

function Env-OrDefault {
    param(
        [hashtable]$Map,
        [string]$Name,
        [string]$DefaultValue
    )
    if ($Map.ContainsKey($Name) -and [string]::IsNullOrWhiteSpace([string]$Map[$Name]) -eq $false) {
        return [string]$Map[$Name]
    }
    return $DefaultValue
}

function Assert-ContainerRunning {
    param([string]$ContainerName)
    $state = & docker inspect -f "{{.State.Running}}" $ContainerName 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Container '$ContainerName' not found. Start docker compose first."
    }
    $running = [string]($state | Select-Object -First 1)
    if ($running.Trim().ToLowerInvariant() -ne "true") {
        throw "Container '$ContainerName' is not running."
    }
}

function Relative-Path {
    param(
        [string]$BasePath,
        [string]$FullPath
    )
    $baseUri = [System.Uri]((Resolve-Path -LiteralPath $BasePath).Path.TrimEnd('\') + '\')
    $fullUri = [System.Uri]((Resolve-Path -LiteralPath $FullPath).Path)
    $rel = $baseUri.MakeRelativeUri($fullUri).ToString()
    return $rel.Replace('/', '\')
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found in PATH."
}

$root = (Resolve-Path -LiteralPath $ProjectRoot).Path
if (-not $OutputRoot) {
    $OutputRoot = Join-Path $root "backups"
}
$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupName = "bssci_backup_$stamp"
$backupDir = Join-Path $OutputRoot $backupName
$dbDir = Join-Path $backupDir "db"
$filesDir = Join-Path $backupDir "files"
$logsDir = Join-Path $backupDir "logs"
$certsDir = Join-Path $backupDir "certs"

New-Item -ItemType Directory -Force -Path $backupDir, $dbDir, $filesDir, $logsDir | Out-Null

$envMap = Read-EnvFile -Path (Join-Path $root ".env")
$dbName = Env-OrDefault -Map $envMap -Name "TIMESCALE_DB" -DefaultValue "bssci"
$dbUser = Env-OrDefault -Map $envMap -Name "TIMESCALE_USER" -DefaultValue "bssci_user"
$dbPassword = Env-OrDefault -Map $envMap -Name "TIMESCALE_PASSWORD" -DefaultValue "change_me"

Write-Step "Using project root: $root"
Write-Step "Backup target: $backupDir"

Assert-ContainerRunning -ContainerName $DbContainer

# 1) Timescale dump
$dumpInContainer = "/tmp/${backupName}.dump"
$dumpOnHost = Join-Path $dbDir "timescaledb.dump"
Write-Step "Creating Timescale dump from container '$DbContainer' (db=$dbName user=$dbUser)"
$dumpCmd = "PGPASSWORD=`"$dbPassword`" pg_dump -U `"$dbUser`" -d `"$dbName`" -Fc -f `"$dumpInContainer`""
& docker exec $DbContainer sh -lc $dumpCmd | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed."
}
& docker cp "${DbContainer}:${dumpInContainer}" $dumpOnHost | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Failed to copy dump from container."
}
& docker exec $DbContainer rm -f $dumpInContainer | Out-Null

# 2) Config and identity files
$fileList = @(
    ".env",
    "docker-compose.yml",
    "bssci_config.py",
    "endpoints.json",
    "endpoints.default.json",
    "base_stations.json",
    "base_stations.default.json",
    "coverage_positions.json",
    "coverage_floorplan.txt",
    "users.json",
    "users.default.json",
    "tenants.json",
    "alerts.json",
    "alerts.default.json",
    "alert_state.json",
    "alert_events.json",
    "viewer_demo_telemetry.py"
)

$copiedFiles = @()
foreach ($name in $fileList) {
    $src = Join-Path $root $name
    if (Test-Path -LiteralPath $src) {
        $dest = Join-Path $filesDir $name
        $destDir = Split-Path -Parent $dest
        if ($destDir -and -not (Test-Path -LiteralPath $destDir)) {
            New-Item -ItemType Directory -Force -Path $destDir | Out-Null
        }
        Copy-Item -LiteralPath $src -Destination $dest -Force
        $copiedFiles += $name
    }
}

$adminAuditSource = Join-Path $root "logs\admin_audit.jsonl"
$copiedLogs = @()
if (Test-Path -LiteralPath $adminAuditSource) {
    $logsDest = Join-Path $logsDir "admin_audit.jsonl"
    Copy-Item -LiteralPath $adminAuditSource -Destination $logsDest -Force
    $copiedLogs += "admin_audit.jsonl"
}

# 3) Certificates
$certSource = Join-Path $root "certs"
$copiedCerts = $false
if (Test-Path -LiteralPath $certSource) {
    New-Item -ItemType Directory -Force -Path $certsDir | Out-Null
    $certItems = Get-ChildItem -LiteralPath $certSource -Force
    foreach ($item in $certItems) {
        Copy-Item -LiteralPath $item.FullName -Destination $certsDir -Recurse -Force
    }
    $copiedCerts = $true
}

# 4) Manifest + checksums
$allFiles = Get-ChildItem -LiteralPath $backupDir -Recurse -File
$checksumLines = @()
foreach ($file in $allFiles) {
    $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $rel = Relative-Path -BasePath $backupDir -FullPath $file.FullName
    $checksumLines += "$hash  $rel"
}
$checksumPath = Join-Path $backupDir "checksums.sha256"
Set-Content -LiteralPath $checksumPath -Value $checksumLines -Encoding UTF8

$manifest = [ordered]@{
    backup_name = $backupName
    created_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    project_root = $root
    db = @{
        container = $DbContainer
        database = $dbName
        user = $dbUser
        dump_file = "db\timescaledb.dump"
    }
    includes = @{
        files = $copiedFiles
        logs = $copiedLogs
        certs = $copiedCerts
    }
}
$manifestPath = Join-Path $backupDir "manifest.json"
$manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

# Recompute checksums after manifest write
$allFiles = Get-ChildItem -LiteralPath $backupDir -Recurse -File
$checksumLines = @()
foreach ($file in $allFiles) {
    if ($file.Name -eq "checksums.sha256") { continue }
    $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    $rel = Relative-Path -BasePath $backupDir -FullPath $file.FullName
    $checksumLines += "$hash  $rel"
}
Set-Content -LiteralPath $checksumPath -Value $checksumLines -Encoding UTF8

$zipPath = $null
if (-not $NoArchive) {
    $zipPath = Join-Path $OutputRoot "$backupName.zip"
    Write-Step "Creating archive: $zipPath"
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Compress-Archive -Path $backupDir -DestinationPath $zipPath -CompressionLevel Optimal -Force
}

Write-Step "Backup completed."
Write-Host "Folder : $backupDir"
if ($zipPath) {
    Write-Host "Archive: $zipPath"
}
