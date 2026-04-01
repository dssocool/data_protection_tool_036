#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AZURITE_DATA="$SCRIPT_DIR/.azurite"

NO_AZURITE=false
for arg in "$@"; do
    if [ "$arg" = "--no-azurite" ]; then
        NO_AZURITE=true
    fi
done

echo "========================================="
echo " Data Protection Tool - Dev Start All"
if [ "$NO_AZURITE" = true ]; then
    echo " (Azurite DISABLED - using cloud Azure)"
fi
echo "========================================="
echo ""

AZURITE_PID=""

if [ "$NO_AZURITE" = false ]; then
    # --- Azurite (Azure Storage Emulator) ---
    echo "[1/4] Starting Azurite (Azure Storage Emulator)..."

    if ! command -v azurite &>/dev/null; then
        echo "      Azurite not found. Installing via npm..."
        npm install -g azurite
    fi

    # Kill any leftover Azurite from a previous run to avoid EADDRINUSE
    if pgrep -f azurite >/dev/null 2>&1; then
        echo "      Killing stale Azurite process..."
        pkill -f azurite 2>/dev/null || true
        sleep 1
    fi

    mkdir -p "$AZURITE_DATA"
    azurite --silent --skipApiVersionCheck --location "$AZURITE_DATA" &
    AZURITE_PID=$!
    echo "      Azurite PID: $AZURITE_PID"

    echo ""
    echo "Waiting 2 seconds for Azurite to initialize..."
    sleep 2

    if ! kill -0 "$AZURITE_PID" 2>/dev/null; then
        echo "ERROR: Azurite failed to start. Check if ports 10000-10002 are in use."
        exit 1
    fi
else
    echo "[1/4] Skipping Azurite (--no-azurite flag set)"
fi

# --- Build Frontend ---
echo "[2/4] Building Frontend..."
FRONTEND_DIR="$SCRIPT_DIR/DataProtectionTool.ControlCenter/frontend"

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    echo "      node_modules not found. Running npm install..."
    npm install --prefix "$FRONTEND_DIR"
fi

npm run build --prefix "$FRONTEND_DIR"

# --- Control Center ---
echo "[3/4] Starting ControlCenter (HTTP on port 5000, gRPC on port 5001)..."

if [ "$NO_AZURITE" = false ]; then
    # Override storage settings to point to Azurite
    export AzureTableStorage__ConnectionString="UseDevelopmentStorage=true"
    export AzureTableStorage__TableName="Clients"
    export AzureBlobStorage__StorageAccount="devstoreaccount1"
    export AzureBlobStorage__Container="preview"
    export AzureBlobStorage__AccessKey="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    echo "      Using Azurite (local emulator) for storage"
else
    echo "      Using appsettings.Development.json for cloud Azure storage"
fi

dotnet run --project "$SCRIPT_DIR/DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj" &
CC_PID=$!
echo "      ControlCenter PID: $CC_PID"

echo ""
echo "Waiting 5 seconds for ControlCenter to initialize..."
sleep 5

# --- Agent ---
echo "[4/4] Starting Agent (gRPC client) in test mode..."
dotnet run --project "$SCRIPT_DIR/DataProtectionTool.Agent/DataProtectionTool.Agent.csproj" -- test &
AGENT_PID=$!
echo "      Agent PID: $AGENT_PID"

echo ""
echo "========================================="
echo " All services started"
if [ -n "$AZURITE_PID" ]; then
    echo "   Azurite PID:       $AZURITE_PID"
fi
echo "   ControlCenter PID: $CC_PID"
echo "   Agent PID:         $AGENT_PID"
echo "========================================="
echo ""
echo "Press Ctrl+C to stop all services."

cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$AGENT_PID" 2>/dev/null || true
    kill "$CC_PID" 2>/dev/null || true
    if [ -n "$AZURITE_PID" ]; then
        kill "$AZURITE_PID" 2>/dev/null || true
        wait "$AZURITE_PID" 2>/dev/null || true
    fi
    wait "$AGENT_PID" 2>/dev/null || true
    wait "$CC_PID" 2>/dev/null || true
    echo "All services stopped."
}

trap cleanup SIGINT SIGTERM

wait
