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

## Windows Local Run Instructions

To run this project on Windows:
1. Install Node.js (v18+) and Python 3.
2. Clone the repository.
3. Run `npm install` to install dependencies.
4. Set up environment variables in a `.env` file (see `.env.example`).
5. Run `npm run dev` to start the application.
6. The application uses SQLite (`local.db`), so no external database setup is required.

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
- **Runtime**: Node.js with Express
- **Language**: TypeScript with ES modules
- **API Design**: RESTful endpoints defined in `/shared/routes.ts` with Zod validation
- **AI Integration**: OpenAI API via Replit AI Integrations for workflow generation

Routes are registered in `/server/routes.ts` and follow a typed API pattern where request/response schemas are shared between client and server.

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
server/               # Express backend
shared/               # Shared types, schemas, routes
  schema.ts           # Drizzle database schema
  routes.ts           # API route definitions
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