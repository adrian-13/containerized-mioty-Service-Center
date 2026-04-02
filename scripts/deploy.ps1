[CmdletBinding()]
param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$Branch = "release",
    [string[]]$ComposeFiles = @("docker-compose.yml"),
    [string]$ComposeProjectName = "",
    [string]$ServiceName = "bssci-service-center",
    [string]$SmokeTestUrl = "http://localhost:5056/login",
    [int]$HealthTimeoutSeconds = 240,
    [int]$SmokeTimeoutSeconds = 60,
    [switch]$BackupBeforeDeploy,
    [switch]$NoPull,
    [switch]$NoBuild,
    [switch]$NoSmokeTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[deploy] $Message"
}

function Assert-CommandExists {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' not found in PATH."
    }
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Display,
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock
    )
    Write-Step $Display
    & $ScriptBlock
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE: $Display"
    }
}

function Get-ComposeBaseArgs {
    param(
        [string]$ProjectName,
        [string[]]$Files
    )
    $args = @("compose")
    if ($ProjectName) {
        $args += @("-p", $ProjectName)
    }
    foreach ($file in $Files) {
        $args += @("-f", $file)
    }
    return $args
}

function Wait-ForHealthyContainer {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerId,
        [int]$TimeoutSeconds = 240
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastStatus = ""
    while ((Get-Date) -lt $deadline) {
        $status = [string](& docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' $ContainerId 2>$null)
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to inspect container health for '$ContainerId'."
        }
        $status = $status.Trim().ToLowerInvariant()
        $lastStatus = $status
        if ($status -eq 'healthy') {
            return
        }
        if ($status -eq 'exited' -or $status -eq 'dead') {
            throw "Container '$ContainerId' stopped while waiting for health."
        }
        Start-Sleep -Seconds 3
    }
    throw "Container '$ContainerId' did not become healthy within ${TimeoutSeconds}s (last status: $lastStatus)."
}

function Assert-HttpSmoke {
    param(
        [string]$Url,
        [int]$TimeoutSeconds
    )
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSeconds
        $code = [int]$response.StatusCode
        if ($code -lt 200 -or $code -ge 400) {
            throw "Unexpected HTTP status $code"
        }
    } catch {
        throw "HTTP smoke test failed for '$Url': $($_.Exception.Message)"
    }
}

Assert-CommandExists -Name "docker"
Assert-CommandExists -Name "git"

$root = (Resolve-Path -LiteralPath $ProjectRoot).Path
Set-Location -LiteralPath $root

foreach ($composeFile in $ComposeFiles) {
    if (-not (Test-Path -LiteralPath (Join-Path $root $composeFile))) {
        throw "Compose file not found: $composeFile"
    }
}

if ($BackupBeforeDeploy) {
    $backupScript = Join-Path $PSScriptRoot "backup.ps1"
    if (-not (Test-Path -LiteralPath $backupScript)) {
        throw "Backup script not found: $backupScript"
    }
    Write-Step "Creating backup before deploy..."
    & $backupScript -ProjectRoot $root
    if ($LASTEXITCODE -ne 0) {
        throw "Pre-deploy backup failed."
    }
}

if (-not $NoPull) {
    Write-Step "Pulling branch '$Branch' with rebase/autostash..."
    & git -C $root pull --rebase --autostash origin $Branch
    if ($LASTEXITCODE -ne 0) {
        throw "git pull failed."
    }
}

$head = [string](& git -C $root rev-parse --short HEAD)
if ($LASTEXITCODE -ne 0) {
    throw "Failed to read git HEAD."
}
Write-Step "Deploying HEAD $head"

$composeArgs = Get-ComposeBaseArgs -ProjectName $ComposeProjectName -Files $ComposeFiles
$upArgs = @()
$upArgs += $composeArgs
$upArgs += @("up", "-d")
if (-not $NoBuild) {
    $upArgs += "--build"
}
$upArgs += "--remove-orphans"

Invoke-NativeCommand -Display ("docker " + ($upArgs -join ' ')) -ScriptBlock { & docker @upArgs }

$psArgs = @()
$psArgs += $composeArgs
$psArgs += @("ps", "-q", $ServiceName)
$containerId = [string](& docker @psArgs)
if ($LASTEXITCODE -ne 0) {
    throw "Failed to resolve service container id for '$ServiceName'."
}
$containerId = $containerId.Trim()
if (-not $containerId) {
    throw "Service container '$ServiceName' not found."
}

Write-Step "Waiting for container '$ServiceName' to become healthy..."
Wait-ForHealthyContainer -ContainerId $containerId -TimeoutSeconds $HealthTimeoutSeconds

if (-not $NoSmokeTest) {
    Write-Step "Running HTTP smoke test: $SmokeTestUrl"
    Assert-HttpSmoke -Url $SmokeTestUrl -TimeoutSeconds $SmokeTimeoutSeconds
}

Write-Step "Deploy completed successfully."
Write-Host "HEAD    : $head"
Write-Host "Service : $ServiceName"
Write-Host "Health  : healthy"
Write-Host "Smoke   : $([bool](-not $NoSmokeTest))"
