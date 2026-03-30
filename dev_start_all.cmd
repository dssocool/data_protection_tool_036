@echo off
REM Launcher: delegates to PowerShell for proper process management
REM Usage: dev_start_all.cmd [--no-azurite]
powershell -ExecutionPolicy Bypass -File "%~dp0dev_start_all.ps1" %*
