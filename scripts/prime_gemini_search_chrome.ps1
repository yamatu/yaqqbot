[CmdletBinding()]
param(
  [string]$UserDataDir = $env:GEMINI_SEARCH_USER_DATA_DIR,
  [int]$DebugPort = $(if ($env:GEMINI_SEARCH_CDP_PORT) { [int]$env:GEMINI_SEARCH_CDP_PORT } else { 9222 }),
  [string]$ProxyServer = $env:GEMINI_SEARCH_PROXY_SERVER
)

$ErrorActionPreference = "Stop"

function Find-ChromeExecutable {
  $candidates = @(
    "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
    "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe",
    "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe",
    "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
    "$env:LOCALAPPDATA\Microsoft\Edge\Application\msedge.exe"
  )

  foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path $candidate)) {
      return $candidate
    }
  }

  $cmd = Get-Command chrome.exe -ErrorAction SilentlyContinue
  if ($cmd) {
    return $cmd.Source
  }
  $cmd = Get-Command msedge.exe -ErrorAction SilentlyContinue
  if ($cmd) {
    return $cmd.Source
  }
  return $null
}

if (-not $env:LOCALAPPDATA) {
  throw "LOCALAPPDATA is not set."
}

$baseDir = Join-Path $env:LOCALAPPDATA "gemini-search-mcp"
if (-not $UserDataDir) {
  $UserDataDir = Join-Path $baseDir "chrome-profile"
}
if (-not $ProxyServer -and $env:SOCKS5_PROXY) {
  $ProxyServer = $env:SOCKS5_PROXY
}

$chrome = Find-ChromeExecutable
if (-not $chrome) {
  throw "Chrome or Edge was not found. Install Chrome with: winget install Google.Chrome"
}

New-Item -ItemType Directory -Force -Path $UserDataDir | Out-Null

$argsList = @(
  "--user-data-dir=$UserDataDir",
  "--remote-debugging-port=$DebugPort",
  "--no-first-run",
  "--new-window",
  "https://www.google.com.hk/search?q=hello&hl=en&gl=us"
)
if ($ProxyServer) {
  $argsList += "--proxy-server=$ProxyServer"
}

Write-Host "Opening Chrome profile for gemini-search-mcp."
Write-Host "Chrome: $chrome"
Write-Host "Profile: $UserDataDir"
Write-Host "CDP: http://127.0.0.1:$DebugPort"
Write-Host "If Google shows CAPTCHA, finish it in this Chrome window."
Write-Host "Keep this Chrome window open if you want to start gemini-search-mcp with CDP_URL."
Start-Process -FilePath $chrome -ArgumentList $argsList
