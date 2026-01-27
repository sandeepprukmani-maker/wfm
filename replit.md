# Orchestrate - Workflow Automation Platform

## Overview

Orchestrate is a workflow automation platform that enables users to create, manage, and execute data workflows through a visual editor and AI-powered chat interface. The application features a React frontend with a Flask Python backend, using SQLite for data persistence. Users can design workflows using a node-based editor (React Flow), store credentials for external services (Airflow, MSSQL), and track execution history with detailed logs.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React 18 with TypeScript
- **Routing**: Wouter for lightweight client-side routing
- **State Management**: TanStack Query (React Query) for server state management
- **UI Components**: shadcn/ui component library built on Radix UI primitives
- **Styling**: Tailwind CSS with CSS variables for theming (light/dark mode support)
- **Workflow Editor**: React Flow for the visual node-based workflow editor
- **Build Tool**: Vite for development and production builds

### Backend Architecture
- **Framework**: Flask (Python) serving as the REST API server
- **Database**: SQLite via SQLAlchemy ORM
- **AI Integration**: OpenAI API for workflow generation from natural language prompts
- **API Pattern**: RESTful endpoints under `/api/*` prefix
- **Development Server**: Python script (`run_dev.py`) orchestrates both Flask (port 5001) and Vite (port 5000)

### Data Storage
- **Primary Database**: SQLite with SQLAlchemy models
- **Schema Definition**: Drizzle ORM schema in `shared/schema.ts` for type safety (used for TypeScript types, actual DB uses SQLAlchemy)
- **Tables**:
  - `workflows`: Stores workflow definitions with nodes/edges JSON
  - `credentials`: Encrypted connection info for external services
  - `executions`: Audit logs of workflow runs with status and results

### API Structure
- Routes defined in `shared/routes.ts` using Zod for validation
- Endpoints:
  - `/api/workflows` - CRUD operations for workflows
  - `/api/workflows/generate` - AI-powered workflow generation
  - `/api/workflows/:id/execute` - Execute a workflow
  - `/api/credentials` - Manage stored credentials
  - `/api/executions` - View execution history

### Development Workflow
- `npm run dev` starts both Flask API and Vite dev server concurrently
- Vite proxies `/api/*` requests to Flask backend
- Hot module replacement enabled for frontend development

## External Dependencies

### Third-Party Services
- **OpenAI API**: Used for AI-powered workflow generation from natural language prompts
  - Configured via `AI_INTEGRATIONS_OPENAI_API_KEY` and `AI_INTEGRATIONS_OPENAI_BASE_URL` environment variables

### External Service Integrations
- **Apache Airflow**: Workflow orchestration platform (credential type supported)
- **Microsoft SQL Server**: Database connectivity (credential type supported)

### Key NPM Dependencies
- `reactflow` - Visual workflow editor
- `@tanstack/react-query` - Server state management
- `axios` - HTTP client
- `date-fns` - Date formatting
- `zod` - Schema validation
- `react-hook-form` - Form handling
- `wouter` - Client-side routing

### Key Python Dependencies
- `flask` - Web framework
- `flask-cors` - CORS handling
- `sqlalchemy` - ORM for SQLite
- `openai` - OpenAI API client
- `requests` - HTTP requests

## Recent Changes (January 2026)

### Airflow 2.7.3 Integration Enhancements
- Added comprehensive Airflow API helper class (`server_py/airflow_api.py`) with 40+ methods
- Implemented 30+ new REST API endpoints for complete DAG management
- Added pre-trigger safety checks: automatically waits for running DAGs to complete and pauses them before workflow execution

### New Workflow Node Types
**DAG Management:**
- `airflow_trigger` - Trigger a DAG run
- `airflow_pause` - Pause a DAG
- `airflow_unpause` - Unpause a DAG
- `airflow_wait_completion` - Wait for DAG runs to complete
- `airflow_ensure_ready` - Ensure DAG is paused and idle

**DAG Status & Monitoring:**
- `airflow_get_dag` - Get DAG details
- `airflow_list_runs` - List DAG runs
- `airflow_get_run` - Get specific run details
- `airflow_log_check` - Check task logs for assertions

**DAG Run Control:**
- `airflow_update_run_state` - Update run state
- `airflow_delete_run` - Delete a DAG run
- `airflow_clear_tasks` - Clear tasks for re-run

**Task Monitoring:**
- `airflow_list_tasks` - List task instances
- `airflow_get_task` - Get task details
- `airflow_get_task_logs` - Get task logs

**Variable & Pool Management:**
- `airflow_get_variable`, `airflow_set_variable`, `airflow_delete_variable`
- `airflow_list_pools`, `airflow_create_pool`

**Health & Diagnostics:**
- `airflow_health` - Get Airflow health status
- `airflow_import_errors` - Get DAG import errors
- `airflow_get_xcom` - Get XCom entries

### Pre-Trigger Safety Checks
Before workflow execution, the system automatically:
1. Extracts all DAG IDs from workflow nodes
2. Checks if any DAGs have running instances
3. Waits for completion (up to 600s timeout)
4. Pauses all DAGs to prevent scheduled runs during workflow execution