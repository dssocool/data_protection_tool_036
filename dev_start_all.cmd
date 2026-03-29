@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "AZURITE_DATA=%SCRIPT_DIR%.azurite"

echo =========================================
echo  Data Protection Tool - Dev Start All
echo =========================================
echo.

REM --- Azurite (Azure Storage Emulator) ---
echo [1/4] Starting Azurite (Azure Storage Emulator)...

where azurite >nul 2>&1
if errorlevel 1 (
    echo       Azurite not found. Installing via npm...
    npm install -g azurite
)

if not exist "%AZURITE_DATA%" mkdir "%AZURITE_DATA%"
start "Azurite" azurite --silent --location "%AZURITE_DATA%"

echo      Waiting 2 seconds for Azurite to initialize...
timeout /t 2 /nobreak >nul

echo [2/4] Starting ControlCenter (HTTP on port 8190, gRPC on port 8191)...
start "ControlCenter" dotnet run --project "%SCRIPT_DIR%DataProtectionTool.ControlCenter\DataProtectionTool.ControlCenter.csproj"

echo      Waiting 5 seconds for ControlCenter to initialize...
timeout /t 5 /nobreak >nul

echo [3/4] Starting Frontend (Vite dev server on port 5173)...
start "Frontend" cmd /c "cd /d "%SCRIPT_DIR%DataProtectionTool.ControlCenter\frontend" && npm run dev"

echo [4/4] Starting Agent (gRPC client) in test mode...
start "Agent" dotnet run --project "%SCRIPT_DIR%DataProtectionTool.Agent\DataProtectionTool.Agent.csproj" -- test

echo.
echo =========================================
echo  All services started in separate windows
echo    - Azurite window
echo    - ControlCenter window
echo    - Frontend window (Vite HMR)
echo    - Agent window
echo.
echo  Frontend (Vite HMR): http://localhost:5173
echo =========================================
echo.
echo Close the individual windows to stop each service.

endlocal
