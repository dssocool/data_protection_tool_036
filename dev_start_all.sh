#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================="
echo " Data Protection Tool - Dev Start All"
echo "========================================="
echo ""

echo "[1/2] Starting ControlCenter (gRPC server on port 5000)..."
dotnet run --project "$SCRIPT_DIR/DataProtectionTool.ControlCenter/DataProtectionTool.ControlCenter.csproj" &
CC_PID=$!
echo "      ControlCenter PID: $CC_PID"

echo ""
echo "Waiting 5 seconds for ControlCenter to initialize..."
sleep 5

echo "[2/2] Starting Agent (gRPC client) in test mode..."
dotnet run --project "$SCRIPT_DIR/DataProtectionTool.Agent/DataProtectionTool.Agent.csproj" -- test &
AGENT_PID=$!
echo "      Agent PID: $AGENT_PID"

echo ""
echo "========================================="
echo " All services started"
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
    wait "$AGENT_PID" 2>/dev/null || true
    wait "$CC_PID" 2>/dev/null || true
    echo "All services stopped."
}

trap cleanup SIGINT SIGTERM

wait
