$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$azuriteData = Join-Path $scriptDir ".azurite"
$frontendDir = Join-Path $scriptDir "DataProtectionTool.ControlCenter.HttpServer\frontend"
$rpcProj     = Join-Path $scriptDir "DataProtectionTool.ControlCenter.RpcServer\DataProtectionTool.ControlCenter.RpcServer.csproj"
$httpProj    = Join-Path $scriptDir "DataProtectionTool.ControlCenter.HttpServer\DataProtectionTool.ControlCenter.HttpServer.csproj"
$agentProj   = Join-Path $scriptDir "DataProtectionTool.Agent\DataProtectionTool.Agent.csproj"

$noAzurite = $args -contains "--no-azurite"
$noTest    = $args -contains "--agent-no-test-mode"
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
    # --- [1/5] Azurite ---
    Write-Host "[1/5] Starting Azurite (Azure Storage Emulator)..."

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
    Write-Host "[1/5] Skipping Azurite (--no-azurite flag set)"
}

# --- [2/5] Build Frontend ---
Write-Host "[2/5] Building Frontend..."

if (-not (Test-Path (Join-Path $frontendDir "node_modules"))) {
    Write-Host "      node_modules not found. Running npm install..."
    Push-Location $frontendDir
    & cmd /c "npm install"
    Pop-Location
}

Push-Location $frontendDir
& cmd /c "npm run build"
Pop-Location

if (-not $noAzurite) {
    $env:AzureTableStorage__ConnectionString = "UseDevelopmentStorage=true"
    $env:AzureTableStorage__TableName = "Clients"
    $env:AzureBlobStorage__StorageAccount = "devstoreaccount1"
    $env:AzureBlobStorage__Container = "preview"
    $env:AzureBlobStorage__AccessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    Write-Host "      Using Azurite (local emulator) for storage"
} else {
    Write-Host "      Using appsettings.Development.json for cloud Azure storage"
}

# --- [3/5] RpcServer ---
Write-Host "[3/5] Starting RpcServer (gRPC on port 8191)..."

$rpc = Start-Process -FilePath "dotnet" `
    -ArgumentList "run","--project",$rpcProj `
    -NoNewWindow -PassThru
$processes += $rpc
Write-Host "      RpcServer PID: $($rpc.Id)"

Write-Host "      Waiting 5 seconds for RpcServer to initialize..."
Start-Sleep -Seconds 5

# --- [4/5] HttpServer ---
Write-Host "[4/5] Starting HttpServer (HTTP on port 8190)..."

$http = Start-Process -FilePath "dotnet" `
    -ArgumentList "run","--project",$httpProj `
    -NoNewWindow -PassThru
$processes += $http
Write-Host "      HttpServer PID: $($http.Id)"

Write-Host "      Waiting 3 seconds for HttpServer to initialize..."
Start-Sleep -Seconds 3

# --- [5/5] Agent ---
if ($noTest) {
    Write-Host "[5/5] Starting Agent (gRPC client) without test mode..."
} else {
    Write-Host "[5/5] Starting Agent (gRPC client) in test mode..."
}

$agentArgs = @("run","--project",$agentProj)
if (-not $noTest) {
    $agentArgs += @("--","test")
}

$agent = Start-Process -FilePath "dotnet" `
    -ArgumentList $agentArgs `
    -NoNewWindow -PassThru
$processes += $agent
Write-Host "      Agent PID: $($agent.Id)"

Write-Host ""
Write-Host "========================================="
Write-Host " All services started"
if (-not $noAzurite) {
    Write-Host "   Azurite PID:    $($azurite.Id)"
}
Write-Host "   RpcServer PID:  $($rpc.Id)"
Write-Host "   HttpServer PID: $($http.Id)"
Write-Host "   Agent PID:      $($agent.Id)"
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
