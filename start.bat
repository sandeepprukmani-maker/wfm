@echo off
REM ─────────────────────────────────────────────────────────────────
REM FlowForge — Local startup script for Windows (no Docker required)
REM Usage: start.bat
REM ─────────────────────────────────────────────────────────────────
setlocal EnableDelayedExpansion

set PORT=%PORT%
if "%PORT%"=="" set PORT=8000

set SCRIPT_DIR=%~dp0
set BACKEND_DIR=%SCRIPT_DIR%backend
set FRONTEND_DIR=%SCRIPT_DIR%frontend
set VENV_DIR=%SCRIPT_DIR%.venv

echo.
echo   FlowForge Starting...
echo   URL: http://localhost:%PORT%
echo.

REM ── Check Python ──────────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: python not found. Install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)

REM ── Check vendor JS files ─────────────────────────────────────────
set VENDOR_DIR=%SCRIPT_DIR%frontend\vendor
set MISSING_VENDORS=0
for %%f in (react.production.min.js react-dom.production.min.js babel.min.js) do (
    if not exist "%VENDOR_DIR%\%%f" (
        echo   MISSING: frontend\vendor\%%f
        set MISSING_VENDORS=1
    )
)
if "%MISSING_VENDORS%"=="1" (
    echo.
    echo   ERROR: Frontend vendor files are missing.
    echo   Run download_vendors.sh from a machine with internet access,
    echo   then copy the populated frontend\vendor\ folder here.
    echo.
    pause
    exit /b 1
)
echo   Vendor JS: OK

REM ── Create virtual environment ────────────────────────────────────
if not exist "%VENV_DIR%" (
    echo   Creating virtual environment...
    python -m venv "%VENV_DIR%"
)
call "%VENV_DIR%\Scripts\activate.bat"

REM ── Install dependencies ──────────────────────────────────────────
echo   Installing / updating dependencies...
pip install --quiet --upgrade pip
pip install --quiet -r "%BACKEND_DIR%\requirements.txt"

REM ── Environment defaults ──────────────────────────────────────────
if "%FLOWFORGE_SECRET_KEY%"=="" set FLOWFORGE_SECRET_KEY=change-me-in-production
if "%FLOWFORGE_SALT%"=="" set FLOWFORGE_SALT=flowforge-salt
if "%FLOWFORGE_DEV_MODE%"=="" set FLOWFORGE_DEV_MODE=true
if "%DATABASE_URL%"=="" set DATABASE_URL=sqlite:///./flowforge.db
REM Set ANTHROPIC_API_KEY before running:
REM   set ANTHROPIC_API_KEY=sk-ant-...

set FLOWFORGE_FRONTEND_PATH=%FRONTEND_DIR%

echo.
echo   FlowForge API  -^>  http://localhost:%PORT%
echo   UI             -^>  http://localhost:%PORT%
echo   Swagger docs   -^>  http://localhost:%PORT%/api/docs
echo.

REM ── Start backend ─────────────────────────────────────────────────
cd /d "%BACKEND_DIR%"
python -m uvicorn main:app --host 0.0.0.0 --port %PORT% --reload --log-level info
