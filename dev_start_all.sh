#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AZURITE_DATA="$SCRIPT_DIR/.azurite"
FRONTEND_DIR="$SCRIPT_DIR/DataProtectionTool.HttpServer/frontend"
RPC_PROJ="$SCRIPT_DIR/DataProtectionTool.RpcServer/DataProtectionTool.RpcServer.csproj"
HTTP_PROJ="$SCRIPT_DIR/DataProtectionTool.HttpServer/DataProtectionTool.HttpServer.csproj"
AGENT_PROJ="$SCRIPT_DIR/DataProtectionTool.Agent/DataProtectionTool.Agent.csproj"

NO_AZURITE=false
NO_TEST=false
for arg in "$@"; do
    if [ "$arg" = "--no-azurite" ]; then
        NO_AZURITE=true
    fi
    if [ "$arg" = "--agent-no-test-mode" ]; then
        NO_TEST=true
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
RPC_PID=""
HTTP_PID=""
AGENT_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    [ -n "$AGENT_PID" ] && kill "$AGENT_PID" 2>/dev/null || true
    [ -n "$HTTP_PID" ]  && kill "$HTTP_PID"  2>/dev/null || true
    [ -n "$RPC_PID" ]   && kill "$RPC_PID"   2>/dev/null || true
    [ -n "$AZURITE_PID" ] && kill "$AZURITE_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "All services stopped."
}

trap cleanup SIGINT SIGTERM

if [ "$NO_AZURITE" = false ]; then
    # --- [1/5] Azurite ---
    echo "[1/5] Starting Azurite (Azure Storage Emulator)..."

    if ! command -v azurite &>/dev/null; then
        echo "      Azurite not found. Installing via npm..."
        npm install -g azurite
    fi

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
    echo "[1/5] Skipping Azurite (--no-azurite flag set)"
fi

# --- [2/5] Build Frontend ---
echo "[2/5] Building Frontend..."

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    echo "      node_modules not found. Running npm install..."
    npm install --prefix "$FRONTEND_DIR"
fi

npm run build --prefix "$FRONTEND_DIR"

if [ "$NO_AZURITE" = false ]; then
    export AzureTableStorage__ConnectionString="UseDevelopmentStorage=true"
    export AzureTableStorage__TableName="Clients"
    echo "      Using Azurite (local emulator) for storage"
else
    echo "      Using appsettings.Development.json for cloud Azure storage"
fi

# --- [3/5] RpcServer ---
echo "[3/5] Starting RpcServer (gRPC on port 8191)..."

dotnet run --project "$RPC_PROJ" &
RPC_PID=$!
echo "      RpcServer PID: $RPC_PID"

echo ""
echo "Waiting 5 seconds for RpcServer to initialize..."
sleep 5

# --- [4/5] HttpServer ---
export RpcServer__Address="http://localhost:5000"

echo "[4/5] Starting HttpServer (HTTP on port 8190)..."

dotnet run --project "$HTTP_PROJ" &
HTTP_PID=$!
echo "      HttpServer PID: $HTTP_PID"

echo ""
echo "Waiting 3 seconds for HttpServer to initialize..."
sleep 3

# --- [5/5] Agent ---
if [ "$NO_TEST" = true ]; then
    echo "[5/5] Starting Agent (gRPC client) without test mode..."
    dotnet run --project "$AGENT_PROJ" &
else
    echo "[5/5] Starting Agent (gRPC client) in test mode..."
    dotnet run --project "$AGENT_PROJ" -- test &
fi
AGENT_PID=$!
echo "      Agent PID: $AGENT_PID"

echo ""
echo "========================================="
echo " All services started"
if [ -n "$AZURITE_PID" ]; then
    echo "   Azurite PID:    $AZURITE_PID"
fi
echo "   RpcServer PID:  $RPC_PID"
echo "   HttpServer PID: $HTTP_PID"
echo "   Agent PID:      $AGENT_PID"
echo "========================================="
echo ""
echo "Press Ctrl+C to stop all services."

wait
