@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "AZURITE_DATA=%SCRIPT_DIR%.azurite"
set "PIDS="

echo =========================================
echo  Data Protection Tool - Dev Start All
echo =========================================
echo.

REM --- Azurite (Azure Storage Emulator) ---
echo [1/5] Starting Azurite (Azure Storage Emulator)...

where azurite >nul 2>&1
if errorlevel 1 (
    echo       Azurite not found. Installing via npm...
    npm install -g azurite
)

if not exist "%AZURITE_DATA%" mkdir "%AZURITE_DATA%"
start /b "" azurite --silent --location "%AZURITE_DATA%"

echo      Waiting 2 seconds for Azurite to initialize...
timeout /t 2 /nobreak >nul

echo [2/5] Building Frontend...
cd /d "%SCRIPT_DIR%DataProtectionTool.ControlCenter\frontend"
if not exist "node_modules" (
    echo       node_modules not found. Running npm install...
    npm install
)
npm run build
cd /d "%SCRIPT_DIR%"

echo [3/5] Starting ControlCenter (HTTP on port 8190, gRPC on port 8191)...
start /b "" dotnet run --project "%SCRIPT_DIR%DataProtectionTool.ControlCenter\DataProtectionTool.ControlCenter.csproj"

echo      Waiting 5 seconds for ControlCenter to initialize...
timeout /t 5 /nobreak >nul

echo [4/5] Starting Frontend (Vite dev server on port 5173)...
start /b "" cmd /c "cd /d "%SCRIPT_DIR%DataProtectionTool.ControlCenter\frontend" && npm run dev"

echo [5/5] Starting Agent (gRPC client) in test mode...
start /b "" dotnet run --project "%SCRIPT_DIR%DataProtectionTool.Agent\DataProtectionTool.Agent.csproj" -- test

echo.
echo =========================================
echo  All services started
echo    - Azurite
echo    - Frontend built to wwwroot
echo    - ControlCenter (port 8190/8191)
echo    - Frontend Vite HMR (port 5173)
echo    - Agent (test mode)
echo.
echo  Frontend (Vite HMR): http://localhost:5173
echo =========================================
echo.
echo Press Ctrl+C to stop all services.
echo.

:wait_loop
timeout /t 3600 /nobreak >nul
goto wait_loop
