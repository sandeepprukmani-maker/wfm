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
  - `/api/workflows/:id/execute` - Execute a workflow (waits for running DAGs to complete)
  - `/api/credentials` - Manage stored credentials
  - `/api/executions` - View execution history
  - `/api/airflow/natural-language` - Natural language DAG operations
  - `/api/airflow/operations` - List all available Airflow operations
  - `/api/airflow/execute` - Execute specific Airflow operations directly
  - `/api/airflow/batch` - Execute multiple Airflow operations in batch

### Airflow Integration (server_py/airflow_api.py)
Comprehensive Airflow 2.7.3 REST API wrapper supporting:
- **DAG Operations**: list, get, pause, unpause, get tasks
- **DAG Runs**: list, trigger, get, update state, delete, clear
- **Task Instances**: list, get, update state, get logs, clear
- **Connections**: list, get, create, update, delete
- **Pools**: list, get, create, update, delete
- **Variables**: list, get, create, update, delete
- **XComs**: list, get
- **Event Logs**: list, get
- **Import Errors**: list, get
- **Health & Monitoring**: health check, version, config
- **Plugins & Providers**: list plugins, list providers

### Natural Language DAG Operations
The `/api/airflow/natural-language` endpoint uses OpenAI to parse natural language commands and execute Airflow operations. Examples:
- "trigger the etl_pipeline dag"
- "pause the daily_report dag"
- "show me all running dag runs"
- "get logs for task extract in dag etl_pipeline"
- "create a variable called api_key with value abc123"
- "clear all failed tasks in the data_sync dag"

### Node Scheduling Feature
All workflow nodes support optional scheduling with:
- **Scheduled Time**: Time picker for setting when the node should run (HH:MM format)
- **Timezone**: Dropdown with 18 major timezones (UTC, US timezones, European, Asian, Australian)
- **Visual Indicator**: Nodes with schedules display a blue clock badge showing time and timezone on the node card
- **Configuration**: Schedule is stored in `node.data.config.scheduledTime` and `node.data.config.timezone`

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