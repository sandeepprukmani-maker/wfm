# Orchestrate - Workflow Automation Platform

A workflow automation platform that enables users to create, manage, and execute data workflows through a visual editor and AI-powered interface.

## System Architecture

- **Frontend**: React 18 with TypeScript, Vite, React Flow, and Shadcn UI.
- **Backend**: Flask (Python) REST API.
- **Database**: SQLite (via SQLAlchemy).
- **AI**: OpenAI API for workflow generation.

## Prerequisites

- Node.js (v18+)
- Python (3.11+)
- npm

## Setup and Installation

1. **Install Frontend Dependencies**:
   ```bash
   npm install
   ```

2. **Install Backend Dependencies**:
   The project uses a Nix-based environment on Replit. For local setup, ensure you have the packages listed in `pyproject.toml` installed:
   ```bash
   pip install flask flask-cors flask-sqlalchemy sqlalchemy openai requests psycopg2-binary
   ```

3. **Environment Variables**:
   Ensure the following environment variables are set:
   - `AI_INTEGRATIONS_OPENAI_API_KEY`: Your OpenAI API key.
   - `AI_INTEGRATIONS_OPENAI_BASE_URL`: OpenAI base URL (optional).
   - `DATABASE_URL`: Connection string for the database.
   - `SESSION_SECRET`: A secret key for Flask sessions.

## Running the Application

To start both the frontend and backend concurrently in development mode:

```bash
npm run dev
```

This command executes `python run_dev.py`, which:
- Starts the **Flask API** on port `5001`.
- Starts the **Vite Dev Server** on port `5000`.

The application will be accessible at `http://localhost:5000`.

## Project Structure

- `client/`: React frontend source code.
- `server_py/`: Flask backend source code and models.
- `shared/`: Shared types and logic.
- `run_dev.py`: Development orchestration script.
- `vite.config.ts`: Vite configuration with API proxy.
