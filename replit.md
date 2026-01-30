# Orchestrator - Airflow Workflow Manager

## Overview
A full-stack application for creating and managing Airflow workflows, with a React/Vite frontend and Flask backend.

## Project Structure
```
├── client/          # React frontend (Vite)
├── server_py/       # Flask backend API
├── shared/          # Shared types/utilities
├── models.py        # SQLAlchemy database models
├── run_dev.py       # Development server runner
├── pyproject.toml   # Python dependencies
├── package.json     # Node.js dependencies
└── requirements.txt # Python requirements
```

## Tech Stack
- **Frontend**: React, Vite, TailwindCSS
- **Backend**: Flask API on port 5001
- **Database**: PostgreSQL (via Flask-SQLAlchemy)

## Running the Application
The application runs via the workflow "Start application" which executes:
```
python run_dev.py
```
This starts:
- Vite dev server on port 5000 (frontend)
- Flask API on port 5001 (backend)

## Database
- PostgreSQL database is configured via `DATABASE_URL` environment variable
- Models are defined in `models.py`

## Features
### SQL Query Assertions
- SQL query nodes support Python assertions to validate query results
- Use `results` variable to access the query result rows (list of dicts)
- Use `count` variable for record count
- Example: `len(results) > 0` or `any(r['value'] > 100 for r in results)`
- Available functions: `any`, `all`, `len`, `sum`, `min`, `max`, `abs`, `round`

### Excel Export
- Query results are automatically exported to Excel with:
  - Auto-fit column widths
  - Yellow header row with bold font
- Download Excel files from the execution logs panel

### Zip Export
- Download all execution results as a zip file containing:
  - Excel files from SQL query nodes (in `excel/` folder)
  - Execution logs (in `logs/` folder) - optional
  - DAG task logs from Airflow log checks (in `logs/` folder)
  - Execution summary JSON
- Two download options:
  - "Zip (with logs)" - includes all logs
  - "Zip (no logs)" - only Excel files and summary

### Database Credentials
- SQL queries can use different database credentials (MSSQL, PostgreSQL)
- Or use the internal database if no credential is selected

## Recent Changes
- 2026-01-28: Added SQL query assertion support, database credential selector, and Excel download capability
- 2026-01-27: Completed migration to Replit environment, installed npm and Python dependencies
