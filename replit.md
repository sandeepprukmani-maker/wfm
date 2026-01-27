# Flask Website Project

## Overview
A Flask web application with PostgreSQL database support, configured for the Replit environment.

## Project Structure
- `main.py` - Entry point for the application
- `app.py` - Flask application configuration and setup
- `models.py` - Database models using SQLAlchemy

## Tech Stack
- **Framework**: Flask 3.1.2
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Server**: Gunicorn (production-ready WSGI server)
- **Authentication**: Flask-Login

## Running the Application
The application runs automatically via the configured workflow using:
```
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection string (auto-configured)
- `SESSION_SECRET` - Flask session secret key

## Recent Changes
- January 27, 2026: Initial project setup and import completed
