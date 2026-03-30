@echo off
REM Launcher: delegates to PowerShell for proper process management
REM Usage: dev_start_all.cmd [--no-azurite] [--no-test]
powershell -ExecutionPolicy Bypass -File "%~dp0dev_start_all.ps1" %*
