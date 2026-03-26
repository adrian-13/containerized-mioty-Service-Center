[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$CommitMessage,
    [string]$ProjectRoot = "",
    [string]$CleanCloneRoot = "",
    [string]$Remote = "origin",
    [string]$Branch = "",
    [string]$ComposeProjectName = "bssci-clean",
    [string[]]$ComposeFiles = @("docker-compose.yml", "docker-compose.test.yml"),
    [string[]]$IncludePaths = @(),
    [string[]]$ExcludePaths = @("__pycache__"),
    [switch]$SkipRebuild,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[publish] $Message"
}

function Format-Command {
    param(
        [string]$Exe,
        [string[]]$Arguments
    )
    $parts = @($Exe)
    foreach ($arg in $Arguments) {
        if ($null -eq $arg) { continue }
        $text = [string]$arg
        if ($text -match '\s') {
            $parts += '"' + $text.Replace('"', '\"') + '"'
        } else {
            $parts += $text
        }
    }
    return ($parts -join " ")
}

function Invoke-External {
    param(
        [string]$Exe,
        [string[]]$Arguments,
        [string]$WorkingDirectory,
        [switch]$AllowFailure,
        [switch]$ReadOnly
    )

    $display = Format-Command -Exe $Exe -Arguments $Arguments
    Write-Step "$display"

    if ($DryRun -and -not $ReadOnly) {
        return @()
    }

    Push-Location $WorkingDirectory
    try {
        $output = & $Exe @Arguments
        $exitCode = $LASTEXITCODE
    } finally {
        Pop-Location
    }

    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "Command failed with exit code ${exitCode}: $display"
    }

    return @($output)
}

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' not found in PATH."
    }
}

function Resolve-AbsolutePath {
    param([string]$Path)
    return [System.IO.Path]::GetFullPath($Path)
}

Assert-Command "git"
Assert-Command "docker"

if (-not $ProjectRoot) {
    $scriptRoot = Split-Path -Parent $PSCommandPath
    $ProjectRoot = Join-Path $scriptRoot ".."
}

$root = Resolve-AbsolutePath -Path $ProjectRoot
if (-not (Test-Path -LiteralPath (Join-Path $root ".git"))) {
    throw "Project root '$root' is not a git repository."
}

if (-not $CleanCloneRoot) {
    $cleanBase = Split-Path (Split-Path (Split-Path $root -Parent) -Parent) -Parent
    $CleanCloneRoot = Join-Path $cleanBase "service-center-clean-release"
}

$cleanRoot = Resolve-AbsolutePath -Path $CleanCloneRoot
if (-not (Test-Path -LiteralPath (Join-Path $cleanRoot ".git"))) {
    throw "Clean clone root '$cleanRoot' is not a git repository."
}

if (-not $Branch) {
    $Branch = [string](Invoke-External -Exe "git" -Arguments @("branch", "--show-current") -WorkingDirectory $root -ReadOnly | Select-Object -First 1)
    $Branch = $Branch.Trim()
}

if (-not $Branch) {
    throw "Could not determine current git branch."
}

Write-Step "Project root: $root"
Write-Step "Clean clone : $cleanRoot"
Write-Step "Branch      : $Branch"

if ($IncludePaths.Count -gt 0) {
    Invoke-External -Exe "git" -Arguments (@("add", "--") + $IncludePaths) -WorkingDirectory $root | Out-Null
} else {
    Invoke-External -Exe "git" -Arguments @("add", "-A") -WorkingDirectory $root | Out-Null
}

foreach ($exclude in $ExcludePaths) {
    $status = Invoke-External -Exe "git" -Arguments @("status", "--short", "--", $exclude) -WorkingDirectory $root -AllowFailure -ReadOnly
    if (@($status).Count -gt 0) {
        Invoke-External -Exe "git" -Arguments @("reset", "HEAD", "--", $exclude) -WorkingDirectory $root | Out-Null
    }
}

$stagedFiles = if ($DryRun) {
    Invoke-External -Exe "git" -Arguments @("status", "--short") -WorkingDirectory $root -ReadOnly |
        ForEach-Object {
            $line = [string]$_
            if ($line.Length -ge 4) { $line.Substring(3).Trim() }
        } |
        Where-Object { $_ }
} else {
    Invoke-External -Exe "git" -Arguments @("diff", "--cached", "--name-only") -WorkingDirectory $root -ReadOnly
}
$hasStagedChanges = @($stagedFiles).Count -gt 0
if ($hasStagedChanges) {
    Write-Step ("Staged files: " + ((@($stagedFiles) | ForEach-Object { $_.Trim() } | Where-Object { $_ }) -join ", "))
    Invoke-External -Exe "git" -Arguments @("commit", "-m", $CommitMessage) -WorkingDirectory $root | Out-Null
    Invoke-External -Exe "git" -Arguments @("push", $Remote, $Branch) -WorkingDirectory $root | Out-Null
} else {
    Write-Step "No staged changes found after applying include/exclude filters. Skipping commit/push and refreshing clean clone only."
}

Invoke-External -Exe "git" -Arguments @("pull", "--rebase", "--autostash", $Remote, $Branch) -WorkingDirectory $cleanRoot | Out-Null

if (-not $SkipRebuild) {
    $composeArgs = @("compose", "-p", $ComposeProjectName)
    foreach ($composeFile in $ComposeFiles) {
        $composePath = Join-Path $cleanRoot $composeFile
        if (Test-Path -LiteralPath $composePath) {
            $composeArgs += @("-f", $composeFile)
        }
    }
    $composeArgs += @("up", "-d", "--build")
    Invoke-External -Exe "docker" -Arguments $composeArgs -WorkingDirectory $cleanRoot | Out-Null
}

$projectHead = [string](Invoke-External -Exe "git" -Arguments @("rev-parse", "--short", "HEAD") -WorkingDirectory $root -ReadOnly | Select-Object -First 1)
$cleanHead = [string](Invoke-External -Exe "git" -Arguments @("rev-parse", "--short", "HEAD") -WorkingDirectory $cleanRoot -ReadOnly | Select-Object -First 1)

Write-Step "Done."
Write-Step "Project HEAD: $($projectHead.Trim())"
Write-Step "Clean   HEAD: $($cleanHead.Trim())"
if (-not $SkipRebuild) {
    Write-Step "Clean test URL: http://localhost:15056"
}
