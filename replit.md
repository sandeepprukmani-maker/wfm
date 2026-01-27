# Orchestrate - Workflow Orchestration Platform

## Overview

Orchestrate is a prompt-driven workflow orchestration system designed to automate Apache Airflow DAG execution, log analysis, SQL-based validations, and Python-based operations. The platform provides a visual workflow editor similar to n8n, allowing users to create, edit, and execute multi-step automation workflows through a drag-and-drop interface.

The system enables users to:
- Monitor and analyze Airflow DAG task logs across multiple retries
- Programmatically trigger DAGs and handle queued runs
- Execute SQL queries in Microsoft SQL Server and analyze results
- Build visual workflows that can be exported as code for version control

## User Preferences

Preferred communication style: Simple, everyday language.

## Local Run Instructions

To run this project locally:
1. Install Node.js (v18+) and Python 3.11+.
2. Clone the repository.
3. Run `npm install` to install frontend dependencies.
4. Run `pip install flask flask-cors sqlalchemy openai requests python-dateutil` for Python dependencies.
5. Set up environment variables for AI integration.
6. Run `python run_dev.py` to start the application.
7. The application uses SQLite (`local.db`), so no external database setup is required.

## Authentication (LLM Gateways)

This project uses a custom Python script (`fetch_token.py`) to retrieve authentication tokens for LLM gateways (like OpenAI). This replaces traditional API key management for these services.

## Workflow Export

Workflows can be exported as Python code for version control. Use the `export_workflow.py` script provided in the root directory.

## System Architecture

### Frontend Architecture
- **Framework**: React 18 with TypeScript
- **Routing**: Wouter (lightweight router)
- **State Management**: TanStack React Query for server state
- **UI Components**: shadcn/ui built on Radix UI primitives
- **Styling**: Tailwind CSS with CSS variables for theming
- **Workflow Editor**: React Flow library for node-based visual editing
- **Build Tool**: Vite with HMR support

The frontend follows a pages-based structure with shared components. Custom hooks in `/client/src/hooks/` handle data fetching for workflows, credentials, and executions.

### Backend Architecture
- **Runtime**: Python 3.11 with Flask
- **Database ORM**: SQLAlchemy
- **API Design**: RESTful endpoints in `/server_py/main.py`
- **AI Integration**: OpenAI API via Replit AI Integrations for workflow generation

Routes are registered in `/server_py/main.py` and the database models are in `/server_py/models.py`. Storage operations are handled in `/server_py/storage.py`.

### Data Storage
- **Database**: SQLite via Drizzle ORM
- **Schema Location**: `/shared/schema.ts`
- **Key Tables**:
  - `workflows`: Stores workflow definitions with React Flow nodes/edges as JSON
  - `credentials`: Encrypted connection info for Airflow and MSSQL
  - `executions`: Workflow run history with status and logs

### Code Organization
```
client/src/           # React frontend
  components/         # Reusable UI components
  pages/              # Route-level components
  hooks/              # Custom React hooks
server_py/            # Python Flask backend
  main.py             # Flask app and API routes
  models.py           # SQLAlchemy database models
  storage.py          # Database storage layer
shared/               # Shared types, schemas, routes (for frontend)
  schema.ts           # Drizzle database schema (frontend types)
  routes.ts           # API route definitions (frontend reference)
run_dev.py            # Development server script
```

## External Dependencies

### Database
- PostgreSQL (required, connection via `DATABASE_URL` environment variable)
- Drizzle ORM for type-safe database operations

### AI Services
- OpenAI API via Replit AI Integrations
- Environment variables: `AI_INTEGRATIONS_OPENAI_API_KEY`, `AI_INTEGRATIONS_OPENAI_BASE_URL`
- Used for workflow generation from natural language prompts

### Authentication
- Replit Auth (OpenID Connect)
- Environment variables: `ISSUER_URL`, `REPL_ID`, `SESSION_SECRET`

### Target Integrations (Workflow Nodes)
- Apache Airflow REST API for DAG triggering and log retrieval
- Microsoft SQL Server for query execution
- Python script execution for custom logic

### Key npm Packages
- `reactflow`: Visual workflow editor
- `drizzle-orm` / `drizzle-zod`: Database ORM with Zod schema generation
- `express-session` / `connect-pg-simple`: Session management
- `openai`: AI integration client
- `@tanstack/react-query`: Server state management
- `zod`: Runtime type validation