$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$azuriteData = Join-Path $scriptDir ".azurite"
$frontendDir = Join-Path $scriptDir "DataProtectionTool.ControlCenter\frontend"
$ccProj      = Join-Path $scriptDir "DataProtectionTool.ControlCenter\DataProtectionTool.ControlCenter.csproj"
$agentProj   = Join-Path $scriptDir "DataProtectionTool.Agent\DataProtectionTool.Agent.csproj"

$noAzurite = $args -contains "--no-azurite"
$processes = @()

function Kill-ProcessTree($pid) {
    Get-CimInstance Win32_Process | Where-Object { $_.ParentProcessId -eq $pid } | ForEach-Object {
        Kill-ProcessTree $_.ProcessId
    }
    Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
}

function Cleanup {
    Write-Host ""
    Write-Host "Shutting down..."
    foreach ($p in $script:processes) {
        if ($p -and !$p.HasExited) {
            try { Kill-ProcessTree $p.Id } catch {}
        }
    }
    Write-Host "All services stopped."
}

Write-Host "========================================="
Write-Host " Data Protection Tool - Dev Start All"
if ($noAzurite) {
    Write-Host " (Azurite DISABLED - using cloud Azure)"
}
Write-Host "========================================="
Write-Host ""

$azurite = $null

if (-not $noAzurite) {
    # --- [1/4] Azurite ---
    Write-Host "[1/4] Starting Azurite (Azure Storage Emulator)..."

    $azuriteCmd = Get-Command azurite -ErrorAction SilentlyContinue
    if (-not $azuriteCmd) {
        Write-Host "      Azurite not found. Installing via npm..."
        & cmd /c "npm install -g azurite"
    }

    if (-not (Test-Path $azuriteData)) { New-Item -ItemType Directory -Path $azuriteData | Out-Null }

    $azurite = Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c","azurite --silent --skipApiVersionCheck --location `"$azuriteData`"" `
        -NoNewWindow -PassThru
    $processes += $azurite
    Write-Host "      Azurite PID: $($azurite.Id)"

    Write-Host "      Waiting 2 seconds for Azurite to initialize..."
    Start-Sleep -Seconds 2
} else {
    Write-Host "[1/4] Skipping Azurite (--no-azurite flag set)"
}

# --- [2/4] Build Frontend ---
Write-Host "[2/4] Building Frontend..."

if (-not (Test-Path (Join-Path $frontendDir "node_modules"))) {
    Write-Host "      node_modules not found. Running npm install..."
    Push-Location $frontendDir
    & cmd /c "npm install"
    Pop-Location
}

Push-Location $frontendDir
& cmd /c "npm run build"
Pop-Location

# --- [3/4] ControlCenter ---
Write-Host "[3/4] Starting ControlCenter (HTTP on port 8190, gRPC on port 8191)..."

if (-not $noAzurite) {
    # Override storage settings to point to Azurite
    $env:AzureTableStorage__ConnectionString = "UseDevelopmentStorage=true"
    $env:AzureTableStorage__TableName = "Clients"
    $env:AzureBlobStorage__StorageAccount = "devstoreaccount1"
    $env:AzureBlobStorage__Container = "preview"
    $env:AzureBlobStorage__AccessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    Write-Host "      Using Azurite (local emulator) for storage"
} else {
    Write-Host "      Using appsettings.Development.json for cloud Azure storage"
}

$cc = Start-Process -FilePath "dotnet" `
    -ArgumentList "run","--project",$ccProj `
    -NoNewWindow -PassThru
$processes += $cc
Write-Host "      ControlCenter PID: $($cc.Id)"

Write-Host "      Waiting 5 seconds for ControlCenter to initialize..."
Start-Sleep -Seconds 5

# --- [4/4] Agent ---
Write-Host "[4/4] Starting Agent (gRPC client) in test mode..."

$agent = Start-Process -FilePath "dotnet" `
    -ArgumentList "run","--project",$agentProj,"--","test" `
    -NoNewWindow -PassThru
$processes += $agent
Write-Host "      Agent PID: $($agent.Id)"

Write-Host ""
Write-Host "========================================="
Write-Host " All services started"
if (-not $noAzurite) {
    Write-Host "   Azurite PID:       $($azurite.Id)"
}
Write-Host "   ControlCenter PID: $($cc.Id)"
Write-Host "   Agent PID:         $($agent.Id)"
Write-Host "========================================="
Write-Host ""
Write-Host "Press Ctrl+C to stop all services."

try {
    while ($true) {
        $allExited = $true
        foreach ($p in $processes) {
            if ($p -and !$p.HasExited) {
                $allExited = $false
                break
            }
        }
        if ($allExited) {
            Write-Host "All processes have exited."
            break
        }
        Start-Sleep -Seconds 2
    }
} finally {
    Cleanup
}
