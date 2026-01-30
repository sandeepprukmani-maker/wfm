# Guide to Run Airflow Workflow Manager Locally on Windows

This guide will help you set up and run this project on your local Windows machine.

## Prerequisites
1. **Python 3.12+**: [Download from python.org](https://www.python.org/downloads/windows/)
2. **Node.js 18+**: [Download from nodejs.org](https://nodejs.org/)
3. **Git**: [Download from git-scm.com](https://git-scm.com/)

## Step 1: Clone the Project
Open PowerShell or Command Prompt and run:
```bash
git clone <your-replit-git-url>
cd <project-directory>
```

## Step 2: Backend Setup (Python)
1. Create a virtual environment:
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install flask flask-cors sqlalchemy openpyxl requests gitpython boto3 flask-sqlalchemy flask-login
   ```
3. Set environment variables (create a `.env` file or set in terminal):
   ```bash
   $env:SESSION_SECRET="your-secret-here"
   $env:DATABASE_URL="sqlite:///local.db"
   ```

## Step 3: Frontend Setup (Node.js)
1. Install npm packages:
   ```bash
   npm install
   ```

## Step 4: Running the Application
You can use the provided runner script:
```bash
python run_dev.py
```
Alternatively, start them in two separate terminals:
- **Terminal 1 (Backend)**:
  ```bash
  .\venv\Scripts\activate
  $env:PORT=5001
  python -m server_py.main
  ```
- **Terminal 2 (Frontend)**:
  ```bash
  npx vite --port 5000
  ```

## Troubleshooting
- **Exit Code 0xC0000409**: This usually indicates a stack buffer overflow or dependency conflict. Ensure all Python packages are correctly installed in your virtual environment.
- **Port Conflicts**: If port 5000 or 5001 is in use, the application might fail to start. Use `netstat -ano | findstr :5000` to find and kill conflicting processes.