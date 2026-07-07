@echo off
setlocal
if "%GEMINI_SEARCH_CDP_PORT%"=="" set GEMINI_SEARCH_CDP_PORT=9222
call "%~dp0prime_gemini_search_chrome.bat"
echo.
echo Finish any Google CAPTCHA in the opened Chrome window, then press Enter.
pause >nul
set CDP_URL=http://127.0.0.1:%GEMINI_SEARCH_CDP_PORT%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_gemini_search_mcp.ps1" %*
