[CmdletBinding()]
param(
  [int]$Port = $(if ($env:GEMINI_SEARCH_PORT) { [int]$env:GEMINI_SEARCH_PORT } else { 8080 }),
  [string]$ListenHost = $env:GEMINI_SEARCH_HOST,
  [string]$UserDataDir = $env:GEMINI_SEARCH_USER_DATA_DIR,
  [string]$ProxyServer = $env:GEMINI_SEARCH_PROXY_SERVER,
  [string]$BrowserChannel = $env:BROWSER_CHANNEL,
  [string]$CDPUrl = $env:CDP_URL,
  [switch]$NoHeadless,
  [switch]$SkipInstall
)

$ErrorActionPreference = "Stop"

function Test-CommandExists {
  param([string]$Name)
  return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-PythonForGeminiSearch {
  $candidates = @(
    @{ Command = "py"; Args = @("-3.12") },
    @{ Command = "py"; Args = @("-3.11") },
    @{ Command = "py"; Args = @("-3.10") },
    @{ Command = "python"; Args = @() },
    @{ Command = "python3"; Args = @() }
  )

  foreach ($candidate in $candidates) {
    $command = $candidate["Command"]
    $arguments = $candidate["Args"]
    if (-not (Test-CommandExists $command)) {
      continue
    }
    try {
      $version = & $command @arguments -c "import sys; print(str(sys.version_info.major) + '.' + str(sys.version_info.minor))"
      if (-not $version) {
        continue
      }
      $parts = $version.Trim().Split(".")
      $major = [int]$parts[0]
      $minor = [int]$parts[1]
      if ($major -gt 3 -or ($major -eq 3 -and $minor -ge 10)) {
        return $candidate
      }
    } catch {
      continue
    }
  }
  return $null
}

if (-not $env:LOCALAPPDATA) {
  throw "LOCALAPPDATA is not set."
}

$baseDir = Join-Path $env:LOCALAPPDATA "gemini-search-mcp"
$venvDir = Join-Path $baseDir "venv"
$geminiSearchExe = Join-Path $venvDir "Scripts\gemini-search.exe"

if (-not $UserDataDir) {
  $UserDataDir = Join-Path $baseDir "chrome-profile"
}
New-Item -ItemType Directory -Force -Path $UserDataDir | Out-Null

if (-not $ProxyServer -and $env:SOCKS5_PROXY) {
  $ProxyServer = $env:SOCKS5_PROXY
}

if (-not (Test-Path $geminiSearchExe)) {
  if ($SkipInstall) {
    throw "gemini-search.exe was not found and -SkipInstall was specified: $geminiSearchExe"
  }

  $python = Get-PythonForGeminiSearch
  if (-not $python) {
    throw @"
Python 3.10+ was not found.
Install it in Windows PowerShell:
  winget install Python.Python.3.12

Then reopen PowerShell and run:
  .\scripts\start_gemini_search_mcp.ps1
"@
  }

  $pythonCommand = $python["Command"]
  $pythonArgs = $python["Args"]

  New-Item -ItemType Directory -Force -Path $baseDir | Out-Null
  Write-Host "Creating gemini-search-mcp venv: $venvDir"
  & $pythonCommand @pythonArgs -m venv $venvDir

  $venvPython = Join-Path $venvDir "Scripts\python.exe"
  Write-Host "Upgrading pip..."
  & $venvPython -m pip install --upgrade pip

  Write-Host "Installing gemini-search-mcp..."
  & $venvPython -m pip install "https://github.com/Sophomoresty/gemini-search-mcp/archive/refs/heads/main.zip"
}

$argsList = @("--port", "$Port", "--user-data-dir", "$UserDataDir")

if ($ListenHost) {
  $argsList += @("--host", $ListenHost)
}
if ($CDPUrl) {
  $argsList += @("--cdp-url", $CDPUrl)
}
if ($BrowserChannel) {
  $argsList += @("--channel", $BrowserChannel)
}
if ($NoHeadless -or $env:HEADLESS -eq "0" -or $env:GEMINI_SEARCH_NO_HEADLESS -eq "1") {
  $argsList += "--no-headless"
}
if ($env:GEMINI_SEARCH_BROWSER_BACKEND) {
  $argsList += @("--browser-backend", $env:GEMINI_SEARCH_BROWSER_BACKEND)
}
if ($ProxyServer) {
  $argsList += @("--proxy-server", $ProxyServer)
}
if ($env:GEMINI_SEARCH_CHROMEDRIVER) {
  $argsList += @("--chromedriver-path", $env:GEMINI_SEARCH_CHROMEDRIVER)
}

Write-Host "Starting gemini-search-mcp: http://127.0.0.1:$Port/v1"
Write-Host "Chrome profile: $UserDataDir"
& $geminiSearchExe @argsList
