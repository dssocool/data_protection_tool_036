@echo off
REM Launcher: delegates to PowerShell for proper process management
powershell -ExecutionPolicy Bypass -File "%~dp0dev_start_all.ps1"
