@echo off
setlocal

echo =========================================
echo  Data Protection Tool - Dev Start All
echo =========================================
echo.

echo [1/2] Starting ControlCenter (gRPC server on port 6000)...
start "ControlCenter" dotnet run --project "%~dp0DataProtectionTool.ControlCenter\DataProtectionTool.ControlCenter.csproj"

echo      Waiting 5 seconds for ControlCenter to initialize...
timeout /t 5 /nobreak >nul

echo [2/2] Starting Agent (gRPC client) in test mode...
start "Agent" dotnet run --project "%~dp0DataProtectionTool.Agent\DataProtectionTool.Agent.csproj" -- test

echo.
echo =========================================
echo  All services started in separate windows
echo    - ControlCenter window
echo    - Agent window
echo =========================================
echo.
echo Close the individual windows to stop each service.

endlocal
