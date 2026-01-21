# Visual Workflow Platform

A production-ready visual workflow orchestration platform that allows users to design, execute, and export complex automated workflows through a drag-and-drop interface.

## Overview

This platform enables users to:
- Visually build workflows by connecting different node types
- Execute workflows with automatic output passing between nodes
- Use LLM-powered code generation for SQL and Python
- Export workflows to executable Python code
- Monitor execution status and logs in real-time

## Architecture

### Backend (Flask + SQLite)
- **server/app.py** - Main Flask application with REST API endpoints
- **server/models.py** - SQLAlchemy models (Workflow, WorkflowExecution, NodeExecution)
- **server/node_executor.py** - Executes individual nodes with variable substitution
- **server/workflow_engine.py** - DAG-based workflow execution engine
- **server/llm_service.py** - OpenAI integration for code generation
- **server/exporter.py** - Export workflows to Python/YAML

### Frontend (React + React Flow)
- **client/src/App.jsx** - Main application component
- **client/src/store.js** - Zustand state management
- **client/src/components/** - UI components (Canvas, Sidebar, NodePanel, etc.)

## Node Types

1. **API Node** - HTTP requests (GET, POST, PUT, DELETE)
2. **SQL Node** - SQLite database queries
3. **Python Node** - Custom Python code execution
4. **Decision Node** - Conditional branching (if/else)
5. **Wait Node** - Delays and loop control
6. **LLM Node** - AI-powered SQL/Python code generation
7. **Airflow Node** - Trigger DAGs, check status, fetch logs

## Output Passing

Nodes can reference outputs from previous nodes using the syntax:
- `{{ node_id.field }}` - Access a specific field
- `{{ node_id.rows[0].column }}` - Access nested data

## API Endpoints

- `GET /api/workflows` - List all workflows
- `POST /api/workflows` - Create workflow
- `GET /api/workflows/:id` - Get workflow
- `PUT /api/workflows/:id` - Update workflow
- `DELETE /api/workflows/:id` - Delete workflow
- `POST /api/workflows/:id/execute` - Execute workflow
- `GET /api/workflows/:id/export/python` - Export to Python
- `POST /api/llm/generate` - Generate code with LLM

## Running the Application

The app runs on port 5000 with Flask serving the built React frontend.

## Database

Uses SQLite (`workflow.db`) for storing:
- Workflows (nodes, edges, metadata)
- Execution history
- Node execution logs and outputs

## Recent Changes

- January 21, 2026: Initial creation with all node types, LLM integration, and visual builder
