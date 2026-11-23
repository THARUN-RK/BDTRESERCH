# ============================================================
# run_all.ps1 — Launch the full Gaming Analytics pipeline
# ============================================================
#
# Opens five PowerShell windows, one per component:
#   1. Kafka KRaft broker       (C:\kafka)
#   2. Kafka consumer           (reads events → SQLite)
#   3. FastAPI backend           (http://127.0.0.1:8000)
#   4. Streamlit dashboard       (http://localhost:8501)
#   5. Kafka producer            (generates gaming events)
#
# Usage:
#   cd C:\Users\tharu\.gemini\antigravity\scratch\BDTRESERCH
#   .\run_all.ps1
#
# Stop everything: close each window (Ctrl+C then close), or
# run:  .\run_all.ps1 -Stop
# ============================================================

param(
    [switch]$Stop
)

# ---- Configuration -----------------------------------------
$ProjectDir = $PSScriptRoot                             # where this script lives
$KafkaHome  = "C:\kafka"                                # short path avoids "input line too long"
$VenvPython = Join-Path $ProjectDir ".venv\Scripts\python.exe"
$VenvActivate = Join-Path $ProjectDir ".venv\Scripts\Activate.ps1"

# ---- Stop mode ---------------------------------------------
if ($Stop) {
    Write-Host "Stopping all Gaming Analytics processes..." -ForegroundColor Yellow

    # Kill known process names
    $targets = @("java", "python", "streamlit")
    foreach ($name in $targets) {
        $procs = Get-Process -Name $name -ErrorAction SilentlyContinue |
            Where-Object { $_.Path -like "*kafka*" -or $_.Path -like "*BDTRESERCH*" -or $_.Path -like "*.venv*" }
        if ($procs) {
            $procs | Stop-Process -Force
            Write-Host "  Stopped $name processes" -ForegroundColor Green
        }
    }
    Write-Host "Done." -ForegroundColor Green
    return
}

# ---- Pre-flight checks -------------------------------------
$errors = @()

if (-not (Test-Path $KafkaHome)) {
    $errors += "Kafka not found at $KafkaHome. Run the Kafka download/extract steps first."
}

if (-not (Test-Path $VenvPython)) {
    $errors += "Python venv not found at $VenvPython. Run:  python -m venv .venv  and  pip install -r requirements.txt"
}

try { java -version 2>&1 | Out-Null } catch {
    $errors += "Java not found on PATH. Kafka requires Java 17+."
}

if ($errors.Count -gt 0) {
    Write-Host "`n--- Pre-flight check FAILED ---" -ForegroundColor Red
    foreach ($e in $errors) { Write-Host "  [X] $e" -ForegroundColor Red }
    Write-Host ""
    return
}

Write-Host ""
Write-Host "====================================" -ForegroundColor Cyan
Write-Host "  Gaming Analytics Pipeline Launcher" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""

# ---- Helper: open a titled PowerShell window ----------------
function Start-Component {
    param(
        [string]$Title,
        [string]$Script
    )
    Write-Host "  Starting: $Title ..." -ForegroundColor Green
    Start-Process powershell.exe -ArgumentList @(
        "-NoExit"
        "-Command"
        "`$Host.UI.RawUI.WindowTitle = '$Title'; $Script"
    )
    Start-Sleep -Seconds 1
}

# ---- 1. Kafka KRaft broker ----------------------------------
# Uses direct java invocation with wildcard classpath to avoid
# the "input line is too long" batch-file bug on Windows.
$brokerScript = @"
Set-Location '$KafkaHome'
`$env:KAFKA_HEAP_OPTS = '-Xms256m -Xmx512m'
Write-Host 'Starting Kafka KRaft broker on localhost:9092 ...' -ForegroundColor Cyan
java -Xms256m -Xmx512m ``
     -cp '.\libs\*' ``
     -Dlog4j.configuration=file:config/log4j.properties ``
     kafka.Kafka .\config\server.properties
"@

Start-Component -Title "Kafka Broker" -Script $brokerScript

# Give the broker a few seconds to boot before starting clients
Write-Host "  Waiting 8 seconds for the broker to initialize..." -ForegroundColor DarkGray
Start-Sleep -Seconds 8

# ---- 2. Kafka consumer --------------------------------------
$consumerScript = @"
Set-Location '$ProjectDir'
. '$VenvActivate'
Write-Host 'Starting Kafka consumer ...' -ForegroundColor Cyan
& '$VenvPython' -m kafka.consumer
"@

Start-Component -Title "Kafka Consumer" -Script $consumerScript

# ---- 3. FastAPI backend -------------------------------------
$apiScript = @"
Set-Location '$ProjectDir'
. '$VenvActivate'
Write-Host 'Starting FastAPI on http://127.0.0.1:8000 ...' -ForegroundColor Cyan
& '$VenvPython' -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
"@

Start-Component -Title "FastAPI Backend" -Script $apiScript

# ---- 4. Streamlit dashboard ---------------------------------
$dashboardScript = @"
Set-Location '$ProjectDir'
. '$VenvActivate'
Write-Host 'Starting Streamlit dashboard on http://localhost:8501 ...' -ForegroundColor Cyan
& '$VenvPython' -m streamlit run dashboard/app.py
"@

Start-Component -Title "Streamlit Dashboard" -Script $dashboardScript

# ---- 5. Kafka producer (continuous event stream) ------------
$producerScript = @"
Set-Location '$ProjectDir'
. '$VenvActivate'
Write-Host 'Starting Kafka producer (continuous, Ctrl+C to stop) ...' -ForegroundColor Cyan
& '$VenvPython' -m kafka.producer --count 0 --rate 5 --players 50 --seed 42
"@

Start-Component -Title "Kafka Producer" -Script $producerScript

# ---- Summary ------------------------------------------------
Write-Host ""
Write-Host "====================================" -ForegroundColor Cyan
Write-Host "  All components launched!" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Kafka Broker:   localhost:9092"
Write-Host "  FastAPI:        http://127.0.0.1:8000"
Write-Host "  Dashboard:      http://localhost:8501"
Write-Host "  API Docs:       http://127.0.0.1:8000/docs"
Write-Host ""
Write-Host "  To stop everything:  .\run_all.ps1 -Stop" -ForegroundColor Yellow
Write-Host "  Or close each window individually with Ctrl+C" -ForegroundColor DarkGray
Write-Host ""

# Open the dashboard in the default browser
Start-Process "http://localhost:8501"
