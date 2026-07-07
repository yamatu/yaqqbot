@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0prime_gemini_search_chrome.ps1" %*
