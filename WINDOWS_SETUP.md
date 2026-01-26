# Orchestrate - Windows Setup & Usage Guide

## Prerequisites
- **Node.js**: v18 or higher.
- **Python**: 3.11 or higher.
- **Dependencies**: `pip install pymssql requests` (for Python-based execution).

## Installation
1. Clone the repository to your Windows machine.
2. Open a terminal in the project root.
3. Run `npm install` to install all JavaScript dependencies.
4. Ensure `fetch_token.py` is configured to return your access tokens.

## Running the Application
To start the development server:
```bash
npm run dev
```
The application will be accessible at `http://localhost:5000`.

## Key Features for Airflow & SQL Users

### 1. Advanced Airflow Integration
- **DAG Triggering**: Use the `airflow_trigger` node. You can provide a custom `conf` JSON object.
- **SLA Monitoring**: Use `airflow_sla_check` to detect missed execution deadlines.
- **Variable Management**: Use `airflow_var_mgmt` to dynamically get or set Airflow variables during a run.
- **Log Matching**: The `airflow_log_check` node can monitor logs for specific patterns and pass success/failure to condition nodes.

### 2. Intelligent SQL & Python Operations
- **SQL Execution**: Visual SQL nodes support standard queries.
- **Python Assertions**: In the SQL node configuration, you can write Python logic (e.g., `any(r['value'] > 100 for r in results)`) to validate data.
- **Context Passing**: Columns from SQL results are automatically available as variables using `{{node_id.column_name}}`.
- **Custom Scripts**: Use the `python_script` node for any complex logic, which supports variable resolution.

### 3. Conditional Orchestration
- Use the `condition` node to create branching logic (e.g., Trigger DAG B if DAG A succeeds, else trigger DAG C).
- Conditions can target any variable in the execution context, including SQL row counts or log match results.

## Deployment & Export
- **Python Export**: Click "Export Python" in the workflow editor to generate a standalone Airflow DAG file.
- **pymssql**: Exported SQL tasks are automatically configured to use `pymssql` for database connections.
- **Variable Handling**: All `{{variable}}` placeholders are resolved at runtime in the exported DAG.

## Troubleshooting
- **Database**: The app uses SQLite (`local.db`) for lightweight local storage.
- **Windows Compatibility**: All scripts use `cross-env` to ensure environment variables are handled correctly on Windows.
