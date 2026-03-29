@echo off
setlocal

echo =========================================
echo  Data Protection Tool - Dev Start All
echo =========================================
echo.

echo [1/3] Starting ControlCenter (HTTP on port 8190, gRPC on port 8191)...
start "ControlCenter" dotnet run --project "%~dp0DataProtectionTool.ControlCenter\DataProtectionTool.ControlCenter.csproj"

echo      Waiting 5 seconds for ControlCenter to initialize...
timeout /t 5 /nobreak >nul

echo [2/3] Starting Frontend (Vite dev server on port 5173)...
start "Frontend" cmd /c "cd /d "%~dp0DataProtectionTool.ControlCenter\frontend" && npm run dev"

echo [3/3] Starting Agent (gRPC client) in test mode...
start "Agent" dotnet run --project "%~dp0DataProtectionTool.Agent\DataProtectionTool.Agent.csproj" -- test

echo.
echo =========================================
echo  All services started in separate windows
echo    - ControlCenter window
echo    - Frontend window (Vite HMR)
echo    - Agent window
echo.
echo  Frontend (Vite HMR): http://localhost:5173
echo =========================================
echo.
echo Close the individual windows to stop each service.

endlocal
