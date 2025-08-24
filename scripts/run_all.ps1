# scripts/run_all.ps1
# One-shot runner:
# Online crawl -> validate (full+sample) -> Offline crawl (golden HTML) -> validate -> restore online fixtures -> pytest -> commit & push
# - Works from repo root or any subfolder; auto cd to repo root (parent of this script).
# - Prefers ACTIVE venv (VIRTUAL_ENV), then venv/, then .venv/, then system python (or -PythonExe override).
# - Array-based ExecPy (no quoting issues).
# - Fail-fast on any non-zero exit.

[CmdletBinding()]
param(
  # 1) Params / flags
  [string] $Dataset    = "10_minutes_air_temperature",
  [int]    $Limit      = 500,
  [switch] $SkipOnline,
  [switch] $SkipOffline,
  [switch] $SkipTests,
  [switch] $NoPush,
  [string] $Message    = "chore: run all checks",
  [string] $PythonExe
)

# 2) Early script config (runs now)
$ErrorActionPreference = "Stop"

# Resolve repo root and make paths relative to it
if ($PSScriptRoot) {
  $repoRoot = Split-Path -Parent $PSScriptRoot
} else {
  $repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
}
Set-Location -Path $repoRoot

# Canonical paths used by steps
$FullOut       = "data/dwd/1_crawl_dwd/${Dataset}_urls.jsonl"
$SampleOut     = "data/dwd/1_crawl_dwd/${Dataset}_urls_sample100.jsonl"
$SchemaPath    = "schemas/dwd/crawler_urls.schema.json"
$OfflineOutDir = ".tmp/golden_out"
$OfflineFull   = Join-Path $OfflineOutDir "${Dataset}_urls.jsonl"
$OfflineSample = Join-Path $OfflineOutDir "${Dataset}_urls_sample100.jsonl"

# 3) Utility functions (definitions only)

function Resolve-PythonPath {
  param([string]$Override)

  # If caller supplied an override, it must resolve to a file path.
  if ($Override) {
    $p = $Override.Trim('"').Trim()
    try {
      $abs = (Resolve-Path -LiteralPath $p -ErrorAction Stop).Path
      return $abs
    } catch {
      throw "PythonExe override not found on disk: '$Override'"
    }
  }

  # Otherwise, auto-detect (active venv > venv > .venv > python/py)
  return Get-Python
}


function Get-Python {
  # 1) Prefer the currently activated venv if present
  if ($env:VIRTUAL_ENV) {
    $active = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if (Test-Path $active) {
      try {
        & $active -c "import sys;print(sys.version.split()[0])" 2>$null
        if ($LASTEXITCODE -eq 0) { return $active }
      } catch {}
    }
  }
  # 2) Common local venv folders (prefer 'venv' over '.venv'), then system
  $candidates = @(
    "venv\Scripts\python.exe",
    ".venv\Scripts\python.exe",
    "python",
    "py -3",
    "py"
  )
  foreach ($c in $candidates) {
    try {
      & $c -c "import sys;print(sys.version.split()[0])" 2>$null
      if ($LASTEXITCODE -eq 0) { return $c }
    } catch {}
  }
  throw "No Python interpreter found (looked for activated venv, then: venv, .venv, python, py)."
}

function Exec {
  param([Parameter(Mandatory)][string]$CmdLine)
  Write-Host ">> $CmdLine" -ForegroundColor Cyan
  cmd /c $CmdLine
  if ($LASTEXITCODE -ne 0) { throw "Command failed: $CmdLine (exit=$LASTEXITCODE)" }
}

function ExecPy {
  param([Parameter(Mandatory)][object[]]$Args)
  $tokens = $Args | ForEach-Object { "$_" }
  $exe = $script:PY
  Write-Host ">> $exe $($tokens -join ' ')" -ForegroundColor Cyan
  & $exe @tokens
  if ($LASTEXITCODE -ne 0) { throw "Python failed: $($tokens -join ' ') (exit=$LASTEXITCODE)" }
}


function Ensure-File {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Expected file not found: $Path"
  }
}

function Validate-UrlsJsonl {
  param([Parameter(Mandatory)][string]$Path)
  Ensure-File -Path $Path
  ExecPy @("-m","app.tools.validate_crawler_urls","--input",$Path,"--schema",$SchemaPath)
}

# 4) Step functions

function Step-Online {
  if ($SkipOnline) { Write-Host "Skipping ONLINE crawl/validate." -ForegroundColor DarkYellow; return }
  Write-Host "`n=== ONLINE: crawl (limit=$Limit) ===" -ForegroundColor Green
  ExecPy @("-m","app.main.run_pipeline","--mode","crawl","--dataset",$Dataset,"--limit",$Limit)

  Write-Host "`n=== ONLINE: validate FULL ===" -ForegroundColor Green
  Validate-UrlsJsonl -Path $FullOut

  Write-Host "`n=== ONLINE: ensure SAMPLE exists ===" -ForegroundColor Green
  if (-not (Test-Path -LiteralPath $SampleOut)) {
    Write-Host "Sample missing -> generating first 100 entries..." -ForegroundColor DarkYellow
    ExecPy @("-m","app.tools.refresh_fixture","--input",$FullOut,"--output",$SampleOut,"--count","100")
  }

  Write-Host "`n=== ONLINE: validate SAMPLE ===" -ForegroundColor Green
  Validate-UrlsJsonl -Path $SampleOut
}

function Step-Offline {
  if ($SkipOffline) { Write-Host "Skipping OFFLINE crawl/validate." -ForegroundColor DarkYellow; return }
  Write-Host "`n=== OFFLINE: crawl from golden HTML (limit=$Limit) ===" -ForegroundColor Green
  New-Item -ItemType Directory -Force -Path $OfflineOutDir | Out-Null
  ExecPy @(
    "-m","app.main.run_pipeline","--mode","crawl","--dataset",$Dataset,
    "--source","offline","--outdir",$OfflineOutDir,"--limit",$Limit
  )

  Write-Host "`n=== OFFLINE: validate FULL ===" -ForegroundColor Green
  Validate-UrlsJsonl -Path $OfflineFull

  Write-Host "`n=== OFFLINE: validate SAMPLE ===" -ForegroundColor Green
  if (-not (Test-Path -LiteralPath $OfflineSample)) {
    Write-Host "Offline sample missing -> generating first 100..." -ForegroundColor DarkYellow
    ExecPy @("-m","app.tools.refresh_fixture","--input",$OfflineFull,"--output",$OfflineSample,"--count","100")
  }
  Validate-UrlsJsonl -Path $OfflineSample
}

function Ensure-Online-Fixtures {
  Write-Host "`n=== POST: ensure ONLINE fixtures exist ===" -ForegroundColor Green

  # Make sure ONLINE FULL exists
  if (-not (Test-Path -LiteralPath $FullOut)) {
    if (Test-Path -LiteralPath $OfflineFull) {
      Write-Host "Online FULL missing -> copying from OFFLINE: $OfflineFull -> $FullOut" -ForegroundColor DarkYellow
      New-Item -ItemType Directory -Force -Path (Split-Path $FullOut) | Out-Null
      Copy-Item -LiteralPath $OfflineFull -Destination $FullOut -Force
    } else {
      throw "Missing FULL output for commit: $FullOut (and no offline fallback found)."
    }
  }

  # Ensure ONLINE SAMPLE exists (prefer regenerating from ONLINE FULL)
  if (-not (Test-Path -LiteralPath $SampleOut)) {
    if (Test-Path -LiteralPath $FullOut) {
      Write-Host "Sample missing -> generating from ONLINE FULL..." -ForegroundColor DarkYellow
      ExecPy @("-m","app.tools.refresh_fixture","--input",$FullOut,"--output",$SampleOut,"--count","100")
    } elseif (Test-Path -LiteralPath $OfflineSample) {
      Write-Host "Online FULL unavailable -> copying OFFLINE SAMPLE: $OfflineSample -> $SampleOut" -ForegroundColor DarkYellow
      New-Item -ItemType Directory -Force -Path (Split-Path $SampleOut) | Out-Null
      Copy-Item -LiteralPath $OfflineSample -Destination $SampleOut -Force
    } else {
      throw "Missing SAMPLE output for commit: $SampleOut (no inputs to regenerate from)."
    }
  }

  # Validate the sample we will commit
  Validate-UrlsJsonl -Path $SampleOut
}

function Step-Tests {
  if ($SkipTests) { Write-Host "Skipping tests." -ForegroundColor DarkYellow; return }
  Write-Host "`n=== TESTS: pytest -q ===" -ForegroundColor Green
  ExecPy @("-m","pytest","-q")
}

function Step-Git {
  if ($NoPush) { Write-Host "Skipping git commit/push." -ForegroundColor DarkYellow; return }
  Write-Host "`n=== GIT: add/commit/push ===" -ForegroundColor Green

  # Stage everything
  Exec "git add -A"
  $status = (git status --porcelain)
  if (-not $status) {
    Write-Host "Nothing to commit. Skipping push." -ForegroundColor DarkYellow
    return
  }

  # First commit attempt (hooks may modify files)
  $committed = $false
  try {
    Exec "git commit -m `"${Message}`""
    $committed = $true
  } catch {
    Write-Host "Commit failed (likely a pre-commit hook modified files). Auto-staging refreshed files..." -ForegroundColor DarkYellow
    Exec "git add -A"
    try {
      Exec "git commit -m `"${Message} (fixtures refreshed)`""
      $committed = $true
    } catch {
      throw "Commit failed after auto-staging hook changes. Please inspect 'git status' and try again."
    }
  }

  if (-not $committed) { throw "Unknown error: commit did not complete." }

  # Push (with rebase fallback)
  try {
    Exec "git push origin main"
  } catch {
    Write-Host "Push failed (likely non-fast-forward). Trying rebase..." -ForegroundColor DarkYellow
    Exec "git pull --rebase origin main"
    Exec "git push origin main"
  }
}

# 5) Main sequence (explicit entry point)

function Main {
  # Decide Python (override vs auto) — keep ONLY the path in $script:PY
  $script:PY = Resolve-PythonPath -Override $PythonExe

  # Print version separately (never store it in $script:PY)
  $pyver = "unknown"
  try { $pyver = (& $script:PY -c "import sys;print(sys.version.split()[0])").Trim() } catch {}

  Write-Host "Using Python: $pyver ($script:PY)" -ForegroundColor DarkCyan
  Write-Host "Repo: $repoRoot" -ForegroundColor DarkCyan
  Write-Host "Dataset: $Dataset | Limit: $Limit" -ForegroundColor DarkCyan
  Write-Host "Flags: SkipOnline=$SkipOnline SkipOffline=$SkipOffline SkipTests=$SkipTests NoPush=$NoPush" -ForegroundColor DarkCyan

  Ensure-File -Path $SchemaPath

  Step-Online
  Step-Offline
  Ensure-Online-Fixtures
  Step-Tests
  Step-Git

  Write-Host "`nAll done" -ForegroundColor Green
}


# Start
Main
