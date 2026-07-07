@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_gemini_search_mcp.ps1" %*
