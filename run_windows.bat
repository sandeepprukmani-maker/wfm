@echo off
echo Starting Orchestrator Setup for Windows...

:: Check for Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH. Please install Python 3.10+.
    pause
    exit /b
)

:: Check for Node.js
node --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Node.js is not installed or not in PATH. Please install Node.js.
    pause
    exit /b
)

echo Installing Python dependencies...
python -m pip install -r requirements.txt

echo Installing Node.js dependencies...
call npm install

echo.
echo Setup complete! Starting the application...
echo The backend will run on port 5001 and frontend on port 5000.
echo Press Ctrl+C to stop.
echo.

python run_dev.py
pause
