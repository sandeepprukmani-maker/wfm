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

## Recent Changes
- 2026-01-27: Completed migration to Replit environment, installed npm and Python dependencies
