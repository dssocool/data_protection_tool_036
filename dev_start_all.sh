#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AZURITE_DATA="/tmp/azurite-data-protection-tool"

echo "========================================="
echo " Data Protection Tool - Dev Start All"
echo "========================================="
echo ""

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
azurite --silent --location "$AZURITE_DATA" &
AZURITE_PID=$!
echo "      Azurite PID: $AZURITE_PID"

echo ""
echo "Waiting 2 seconds for Azurite to initialize..."
sleep 2

if ! kill -0 "$AZURITE_PID" 2>/dev/null; then
    echo "ERROR: Azurite failed to start. Check if ports 10000-10002 are in use."
    exit 1
fi

# --- Control Center ---
echo "[2/4] Starting ControlCenter (HTTP on port 8190, gRPC on port 8191)..."
dotnet run --project "$SCRIPT_DIR/DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj" &
CC_PID=$!
echo "      ControlCenter PID: $CC_PID"

echo ""
echo "Waiting 5 seconds for ControlCenter to initialize..."
sleep 5

# --- Frontend (Vite dev server) ---
echo "[3/4] Starting Frontend (Vite dev server on port 5173)..."
FRONTEND_DIR="$SCRIPT_DIR/DataProtectionTool.ControlCenter/frontend"

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    echo "      node_modules not found. Running npm install..."
    npm install --prefix "$FRONTEND_DIR"
fi

npm run dev --prefix "$FRONTEND_DIR" &
FRONTEND_PID=$!
echo "      Frontend PID: $FRONTEND_PID"

# --- Agent ---
echo "[4/4] Starting Agent (gRPC client) in test mode..."
dotnet run --project "$SCRIPT_DIR/DataProtectionTool.Agent/DataProtectionTool.Agent.csproj" -- test &
AGENT_PID=$!
echo "      Agent PID: $AGENT_PID"

echo ""
echo "========================================="
echo " All services started"
echo "   Azurite PID:       $AZURITE_PID"
echo "   ControlCenter PID: $CC_PID"
echo "   Frontend PID:      $FRONTEND_PID"
echo "   Agent PID:         $AGENT_PID"
echo ""
echo " Frontend (Vite HMR): http://localhost:5173"
echo "========================================="
echo ""
echo "Press Ctrl+C to stop all services."

cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$AGENT_PID" 2>/dev/null || true
    kill "$FRONTEND_PID" 2>/dev/null || true
    kill "$CC_PID" 2>/dev/null || true
    kill "$AZURITE_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
    wait "$FRONTEND_PID" 2>/dev/null || true
    wait "$CC_PID" 2>/dev/null || true
    wait "$AZURITE_PID" 2>/dev/null || true
    echo "All services stopped."
}

trap cleanup SIGINT SIGTERM

wait
