[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath,
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$DbContainer = "bssci-timescaledb",
    [string]$AppContainer = "bssci-service-center",
    [switch]$SkipDb,
    [switch]$SkipFiles,
    [switch]$NoStart,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[restore] $Message"
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

function Assert-ContainerExists {
    param([string]$ContainerName)
    $state = & docker inspect -f "{{.State.Status}}" $ContainerName 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Container '$ContainerName' not found."
    }
    return [string]($state | Select-Object -First 1)
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

function Resolve-BackupRoot {
    param([string]$Path)
    $resolved = [System.IO.Path]::GetFullPath((Resolve-Path -LiteralPath $Path).Path)
    if (Test-Path -LiteralPath $resolved -PathType Container) {
        if (Test-Path -LiteralPath (Join-Path $resolved "manifest.json")) {
            return @{ Root = $resolved; Temp = $null }
        }
        $candidate = Get-ChildItem -LiteralPath $resolved -Directory | Where-Object {
            Test-Path -LiteralPath (Join-Path $_.FullName "manifest.json")
        } | Select-Object -First 1
        if ($candidate) {
            return @{ Root = $candidate.FullName; Temp = $null }
        }
        throw "Could not find manifest.json in '$resolved'."
    }

    if ([System.IO.Path]::GetExtension($resolved).ToLowerInvariant() -ne ".zip") {
        throw "BackupPath must be a backup directory or .zip archive."
    }

    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("bssci_restore_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    Expand-Archive -LiteralPath $resolved -DestinationPath $tmp -Force

    if (Test-Path -LiteralPath (Join-Path $tmp "manifest.json")) {
        return @{ Root = $tmp; Temp = $tmp }
    }
    $candidate = Get-ChildItem -LiteralPath $tmp -Directory | Where-Object {
        Test-Path -LiteralPath (Join-Path $_.FullName "manifest.json")
    } | Select-Object -First 1
    if ($candidate) {
        return @{ Root = $candidate.FullName; Temp = $tmp }
    }
    throw "No valid backup structure found inside zip archive."
}

function Verify-Checksums {
    param([string]$BackupRoot)
    $checksumsPath = Join-Path $BackupRoot "checksums.sha256"
    if (-not (Test-Path -LiteralPath $checksumsPath)) {
        Write-Step "checksums.sha256 not found, skipping checksum verification."
        return
    }
    Write-Step "Verifying checksums..."
    $errors = @()
    foreach ($line in Get-Content -LiteralPath $checksumsPath) {
        $trimmed = $line.Trim()
        if (-not $trimmed) { continue }
        $parts = $trimmed -split "\s{2,}", 2
        if ($parts.Count -ne 2) { continue }
        $expected = $parts[0].ToLowerInvariant()
        $relPath = $parts[1].Trim()
        $filePath = Join-Path $BackupRoot $relPath
        if (-not (Test-Path -LiteralPath $filePath)) {
            $errors += "Missing file: $relPath"
            continue
        }
        $actual = (Get-FileHash -LiteralPath $filePath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($expected -ne $actual) {
            $errors += "Checksum mismatch: $relPath"
        }
    }
    if ($errors.Count -gt 0) {
        throw ("Checksum verification failed:`n - " + ($errors -join "`n - "))
    }
    Write-Step "Checksum verification successful."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found in PATH."
}

$root = (Resolve-Path -LiteralPath $ProjectRoot).Path
$resolvedBackup = Resolve-BackupRoot -Path $BackupPath
$backupRoot = [string]$resolvedBackup.Root
$tempPath = $resolvedBackup.Temp

try {
    Write-Step "Project root: $root"
    Write-Step "Backup source: $backupRoot"

    Verify-Checksums -BackupRoot $backupRoot

    if (-not $Force) {
        $answer = Read-Host "Restore will overwrite runtime files and database. Continue? (yes/no)"
        if ($answer.Trim().ToLowerInvariant() -notin @("y", "yes")) {
            Write-Step "Restore canceled by user."
            exit 1
        }
    }

    $appStatus = Assert-ContainerExists -ContainerName $AppContainer
    $dbStatus = Assert-ContainerExists -ContainerName $DbContainer
    $appWasRunning = ($appStatus.Trim().ToLowerInvariant() -eq "running")
    if ($dbStatus.Trim().ToLowerInvariant() -ne "running") {
        throw "Database container '$DbContainer' must be running for restore."
    }

    $preRestoreDir = Join-Path $root "backups\pre_restore_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    New-Item -ItemType Directory -Force -Path $preRestoreDir | Out-Null
    Write-Step "Creating pre-restore safety copy: $preRestoreDir"

    if ($appWasRunning) {
        Write-Step "Stopping app container '$AppContainer'"
        & docker stop $AppContainer | Out-Null
    }

        if (-not $SkipFiles) {
            $filesSource = Join-Path $backupRoot "files"
            if (Test-Path -LiteralPath $filesSource) {
                $preFiles = Join-Path $preRestoreDir "files"
                New-Item -ItemType Directory -Force -Path $preFiles | Out-Null

                foreach ($file in Get-ChildItem -LiteralPath $filesSource -Recurse -File) {
                    $relPath = Relative-Path -BasePath $filesSource -FullPath $file.FullName
                    $target = Join-Path $root $relPath
                    $targetDir = Split-Path -Parent $target
                    if ($targetDir -and -not (Test-Path -LiteralPath $targetDir)) {
                        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
                    }
                    if (Test-Path -LiteralPath $target) {
                        $preTarget = Join-Path $preFiles $relPath
                        $preTargetDir = Split-Path -Parent $preTarget
                        if ($preTargetDir -and -not (Test-Path -LiteralPath $preTargetDir)) {
                            New-Item -ItemType Directory -Force -Path $preTargetDir | Out-Null
                        }
                        Copy-Item -LiteralPath $target -Destination $preTarget -Force
                    }
                    Copy-Item -LiteralPath $file.FullName -Destination $target -Force
                }
                Write-Step "Configuration and identity files restored."
            } else {
            Write-Step "No files/ directory in backup. Skipping config restore."
        }

        $certSource = Join-Path $backupRoot "certs"
        if (Test-Path -LiteralPath $certSource) {
            $targetCerts = Join-Path $root "certs"
            $preCerts = Join-Path $preRestoreDir "certs"
            if (Test-Path -LiteralPath $targetCerts) {
                Copy-Item -LiteralPath $targetCerts -Destination $preCerts -Recurse -Force
            }
            New-Item -ItemType Directory -Force -Path $targetCerts | Out-Null
            Get-ChildItem -LiteralPath $targetCerts -Force | ForEach-Object {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            }
            $certItems = Get-ChildItem -LiteralPath $certSource -Force
            foreach ($item in $certItems) {
                Copy-Item -LiteralPath $item.FullName -Destination $targetCerts -Recurse -Force
            }
            Write-Step "Certificates restored."
        } else {
            Write-Step "No certs/ directory in backup. Skipping certificate restore."
        }

        $logsSource = Join-Path $backupRoot "logs"
        if (Test-Path -LiteralPath $logsSource) {
            $targetLogs = Join-Path $root "logs"
            $preLogs = Join-Path $preRestoreDir "logs"
            if (Test-Path -LiteralPath $targetLogs) {
                Copy-Item -LiteralPath $targetLogs -Destination $preLogs -Recurse -Force
            }
            New-Item -ItemType Directory -Force -Path $targetLogs | Out-Null
            Get-ChildItem -LiteralPath $targetLogs -Force | ForEach-Object {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            }
            Get-ChildItem -LiteralPath $logsSource -Recurse -File | ForEach-Object {
                $relPath = Relative-Path -BasePath $logsSource -FullPath $_.FullName
                $dest = Join-Path $targetLogs $relPath
                $destDir = Split-Path -Parent $dest
                if ($destDir -and -not (Test-Path -LiteralPath $destDir)) {
                    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
                }
                Copy-Item -LiteralPath $_.FullName -Destination $dest -Force
            }
            Write-Step "Log fallback files restored."
        } else {
            Write-Step "No logs/ directory in backup. Skipping log restore."
        }
    } else {
        Write-Step "Skipping file restore by request."
    }

    if (-not $SkipDb) {
        $dumpPath = Join-Path $backupRoot "db\timescaledb.dump"
        if (-not (Test-Path -LiteralPath $dumpPath)) {
            throw "Database dump not found: $dumpPath"
        }

        $envMap = Read-EnvFile -Path (Join-Path $root ".env")
        $dbName = Env-OrDefault -Map $envMap -Name "TIMESCALE_DB" -DefaultValue "bssci"
        $dbUser = Env-OrDefault -Map $envMap -Name "TIMESCALE_USER" -DefaultValue "bssci_user"
        $dbPassword = Env-OrDefault -Map $envMap -Name "TIMESCALE_PASSWORD" -DefaultValue "change_me"

        $dumpInContainer = "/tmp/bssci_restore.dump"
        Write-Step "Copying dump to database container '$DbContainer'"
        & docker cp $dumpPath "${DbContainer}:${dumpInContainer}" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy dump into container."
        }

        $existsCmd = "PGPASSWORD=`"$dbPassword`" psql -U `"$dbUser`" -d postgres -tAc `"SELECT 1 FROM pg_database WHERE datname='$dbName'`""
        $exists = & docker exec $DbContainer sh -lc $existsCmd
        $dbExists = ([string]($exists | Select-Object -First 1)).Trim() -eq "1"
        if (-not $dbExists) {
            $createCmd = "PGPASSWORD=`"$dbPassword`" createdb -U `"$dbUser`" `"$dbName`""
            & docker exec $DbContainer sh -lc $createCmd | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to create database '$dbName'."
            }
        }

        Write-Step "Restoring Timescale database '$dbName'"
        $restoreCmd = "PGPASSWORD=`"$dbPassword`" pg_restore -U `"$dbUser`" -d `"$dbName`" --clean --if-exists --no-owner --no-privileges `"$dumpInContainer`""
        & docker exec $DbContainer sh -lc $restoreCmd | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Database restore failed."
        }
        & docker exec $DbContainer rm -f $dumpInContainer | Out-Null
        Write-Step "Database restore completed."
    } else {
        Write-Step "Skipping database restore by request."
    }

    if (-not $NoStart -and $appWasRunning) {
        Write-Step "Starting app container '$AppContainer'"
        & docker start $AppContainer | Out-Null
    }

    Write-Step "Restore completed successfully."
    Write-Host "Pre-restore copy: $preRestoreDir"
}
finally {
    if ($tempPath -and (Test-Path -LiteralPath $tempPath)) {
        Remove-Item -LiteralPath $tempPath -Recurse -Force
    }
}
