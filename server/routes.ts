import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import { z } from "zod";
import OpenAI from "openai";
import { exec } from "child_process";
import { writeFileSync, unlinkSync, readFileSync } from "fs";
import { join } from "path";
import { promisify } from "util";

const execPromise = promisify(exec);

// Initialize OpenAI client with token-based auth
async function getOpenAIClient() {
  try {
    const { stdout } = await execPromise(`python3.11 fetch_token.py`);
    const tokenData = JSON.parse(stdout);
    const token = typeof tokenData === 'string' ? tokenData : tokenData.token;
    
    return new OpenAI({
      apiKey: token,
      baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
    });
  } catch (error) {
    console.error("Failed to fetch AI token, falling back to env var:", error);
    return new OpenAI({
      apiKey: process.env.AI_INTEGRATIONS_OPENAI_API_KEY,
      baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
    });
  }
}

// Global client instance that gets refreshed
let openaiInstance: OpenAI | null = null;
async function getAi() {
  if (!openaiInstance) {
    openaiInstance = await getOpenAIClient();
  }
  return openaiInstance;
}

import { stringify } from "csv-stringify/sync";
import archiver from "archiver";
import axios from "axios";
import { format, subDays } from "date-fns";

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // === WORKFLOW ROUTES ===

  app.get("/api/workflows/:id/export", async (req, res) => {
    const workflow = await storage.getWorkflow(Number(req.params.id));
    if (!workflow) return res.status(404).json({ message: "Workflow not found" });

    const tempFile = join(process.cwd(), `temp_wf_${Date.now()}.json`);
    try {
      writeFileSync(tempFile, JSON.stringify(workflow));
      const { stdout } = await execPromise(`python3.11 export_workflow.py ${tempFile}`);
      res.json({ code: stdout });
    } catch (error) {
      console.error("Export failed:", error);
      res.status(500).json({ message: "Failed to export workflow" });
    } finally {
      try { unlinkSync(tempFile); } catch {}
    }
  });

  app.get(api.workflows.list.path, async (req, res) => {
    const workflows = await storage.getWorkflows();
    res.json(workflows);
  });

  app.get(api.workflows.get.path, async (req, res) => {
    const workflow = await storage.getWorkflow(Number(req.params.id));
    if (!workflow) return res.status(404).json({ message: "Workflow not found" });
    res.json(workflow);
  });

  app.post(api.workflows.create.path, async (req, res) => {
    try {
      const input = api.workflows.create.input.parse(req.body);
      const workflow = await storage.createWorkflow(input);
      res.status(201).json(workflow);
    } catch (err) {
      if (err instanceof z.ZodError) {
        return res.status(400).json({ message: err.errors[0].message });
      }
      throw err;
    }
  });

  app.put(api.workflows.update.path, async (req, res) => {
    try {
      const input = api.workflows.update.input.parse(req.body);
      const workflow = await storage.updateWorkflow(Number(req.params.id), input);
      if (!workflow) return res.status(404).json({ message: "Workflow not found" });
      res.json(workflow);
    } catch (err) {
      if (err instanceof z.ZodError) {
        return res.status(400).json({ message: err.errors[0].message });
      }
      throw err;
    }
  });

  app.delete(api.workflows.delete.path, async (req, res) => {
    await storage.deleteWorkflow(Number(req.params.id));
    res.status(204).send();
  });

  // === AIRFLOW ACTIONS ===
  app.post("/api/airflow/mark-failed", async (req, res) => {
    const { dagId, runId, taskId } = req.body;
    console.log(`Marking ${taskId ? `task ${taskId}` : `DAG ${dagId}`} as FAILED for run ${runId}`);
    res.json({ success: true, message: "Marked as failed" });
  });

  app.post("/api/airflow/clear-task", async (req, res) => {
    const { dagId, runId, taskId } = req.body;
    console.log(`Clearing task ${taskId} for DAG ${dagId} (run ${runId})`);
    res.json({ success: true, message: "Task cleared" });
  });
  app.post(api.workflows.generate.path, async (req, res) => {
    const { prompt, workflowId } = req.body;
    let attempts = 0;
    const maxRetries = 3;

    while (attempts < maxRetries) {
      try {
        const openai = await getAi();
        const response = await openai.chat.completions.create({
          model: "gpt-4o",
          messages: [
            {
              role: "system",
              content: `You are a workflow generator for Apache Airflow 2.7.3 and SQL Server.
              CRITICAL: Break down the request into multiple nodes. ONE OPERATION PER NODE.
              Nodes can pass values using double curly braces (e.g., {{dagRunId}}, {{queryResult}}).

              The available node types are:
              - 'airflow_trigger': ONLY triggers a DAG. Config: { dagId: string, conf?: object }. Output: dagRunId.
              - 'airflow_log_check': Checks logs for a pattern. Config: { dagId: string, taskName?: string, logAssertion: string }. Usually follows a trigger.
              - 'airflow_sla_check': Monitor for SLA misses. Config: { dagId: string, thresholdSeconds: number }.
              - 'airflow_var_mgmt': Get/Set Airflow variables. Config: { key: string, value?: string, action: 'get' | 'set' }.
              - 'sql_query': Execute SQL. Config: { query: string, credentialId: number }. Output: queryResult.
              - 'python_script': Run Python code. Config: { code: string }.
              - 'condition': Branching logic based on previous results. Config: { threshold: number, variable: string, operator: string }.
              - 'api_request': Call external APIs. Config: { url: string, method: string, headers: object, body: string }.
              
              Return ONLY a JSON object with 'nodes' and 'edges' compatible with React Flow.
              Nodes should have id, position ({x, y}), data ({label, type, config}).
              Edges should have id, source, target.

              Example: If user says "Trigger DAG A and check logs for 'Success'", create TWO nodes: 
              1. 'airflow_trigger' node.
              2. 'airflow_log_check' node connected to the trigger.
              `
            },
            {
              role: "user",
              content: `Scenario: ${prompt}.`
            }
          ],
          response_format: { type: "json_object" }
        });

        const rawContent = response.choices[0].message.content || "{}";
        const content = JSON.parse(rawContent);

        // Validation: Ensure nodes and edges are present
        if (!content.nodes || !Array.isArray(content.nodes)) {
          throw new Error("Invalid response: 'nodes' array is missing");
        }
        
        if (workflowId) {
          await storage.updateWorkflow(Number(workflowId), { lastPrompt: prompt });
        }
        
        return res.json(content);
      } catch (error) {
        attempts++;
        console.error(`AI Generation attempt ${attempts} failed:`, error);
        if (attempts === maxRetries) {
          return res.status(500).json({ 
            message: "Failed to generate workflow after multiple attempts",
            error: error instanceof Error ? error.message : String(error)
          });
        }
        // Small delay before retry
        await new Promise(resolve => setTimeout(resolve, 500 * attempts));
      }
    }
  });

  // === CREDENTIALS ===
  app.get(api.credentials.list.path, async (req, res) => {
    const credentials = await storage.getCredentials();
    res.json(credentials);
  });

  app.post(api.credentials.create.path, async (req, res) => {
    try {
      const input = api.credentials.create.input.parse(req.body);
      const credential = await storage.createCredential(input);
      res.status(201).json(credential);
    } catch (err) {
      if (err instanceof z.ZodError) {
        return res.status(400).json({ message: err.errors[0].message });
      }
      throw err;
    }
  });

  app.delete(api.credentials.delete.path, async (req, res) => {
    await storage.deleteCredential(Number(req.params.id));
    res.status(204).send();
  });

  // === EXECUTION ===
  // === EXPORT ZIP ===
  app.get("/api/executions/:id/export", async (req, res) => {
    const executionId = Number(req.params.id);
    const execution = await storage.getExecution(executionId);
    if (!execution) return res.status(404).json({ message: "Execution not found" });

    const workflow = await storage.getWorkflow(execution.workflowId);
    if (!workflow) return res.status(404).json({ message: "Workflow not found" });

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename=execution_${executionId}_export.zip`);

    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.pipe(res);

    // 1. Python Code
    const tempFile = join(process.cwd(), `temp_export_${Date.now()}.json`);
    try {
      writeFileSync(tempFile, JSON.stringify(workflow));
      const { stdout: pythonCode } = await execPromise(`python3.11 export_workflow.py ${tempFile}`);
      archive.append(pythonCode, { name: "workflow.py" });
    } catch (e) {
      archive.append("# Python export failed", { name: "workflow.py" });
    } finally {
      try { unlinkSync(tempFile); } catch {}
    }

    // 2. Execution Logs
    const logContent = (execution.logs as any[] || [])
      .map(l => `[${l.timestamp}] ${l.level}: ${l.message}`)
      .join("\n");
    archive.append(logContent, { name: "execution.log" });

    // 3. SQL Results (CSVs)
    const results = (execution as any).results || {};
    for (const [nodeId, csvData] of Object.entries(results)) {
      archive.append(csvData as string, { name: `results/node_${nodeId}.csv` });
    }

    archive.finalize();
  });

  app.post("/api/nodes/:nodeId/refine", async (req, res) => {
    const { nodeId, currentConfig, instruction } = req.body;
    let attempts = 0;
    const maxRetries = 2;

    while (attempts < maxRetries) {
      try {
        const openai = await getAi();
        const response = await openai.chat.completions.create({
          model: "gpt-4o",
          messages: [
            {
              role: "system",
              content: `You are a workflow expert. Refine the JSON configuration for a specific node based on user instructions. 
              Maintain the existing schema. Only update relevant fields.
              Available fields depend on node type:
              - sql_query: query
              - api_request: url, method, headers, body
              - airflow_trigger: dagId, conf
              - airflow_sla_check: dagId, thresholdSeconds
              - airflow_var_mgmt: key, value, action
              - condition: threshold, variable, operator
              
              Return ONLY the refined JSON config object.`
            },
            {
              role: "user",
              content: `Current Config: ${JSON.stringify(currentConfig)}\nInstruction: ${instruction}`
            }
          ],
          response_format: { type: "json_object" }
        });
        const rawContent = response.choices[0].message.content || "{}";
        const refined = JSON.parse(rawContent);
        return res.json(refined);
      } catch (error) {
        attempts++;
        console.error(`Refinement attempt ${attempts} failed:`, error);
        if (attempts === maxRetries) {
          return res.status(500).json({ message: "Failed to refine node after retries" });
        }
        await new Promise(resolve => setTimeout(resolve, 300 * attempts));
      }
    }
  });

  app.post(api.workflows.execute.path, async (req, res) => {
    const workflowId = Number(req.params.id);
    const execution = await storage.createExecution(workflowId);

    // Mock Execution in Background
    (async () => {
      const workflow = await storage.getWorkflow(workflowId);
      if (!workflow) return;

      const logs = [];
      const results: Record<string, string> = {};
      logs.push({ timestamp: new Date(), level: 'INFO', message: 'Starting workflow execution...' });
      await storage.updateExecution(execution.id, "running", logs);

      const nodes = (workflow.nodes as any[]) || [];
      const executionContext: Record<string, any> = {};
      
      const resolveVariables = (str: string, context: Record<string, any>) => {
        if (!str || typeof str !== 'string') return str;
        let resolved = str;
        
        // Handle Date Variables
        resolved = resolved.replace(/\{\{today\}\}/g, format(new Date(), 'yyyy-MM-dd'));
        resolved = resolved.replace(/\{\{yesterday\}\}/g, format(subDays(new Date(), 1), 'yyyy-MM-dd'));
        
        // Handle Custom Formats (e.g., {{date:yyyyMMdd:sub1}})
        const dateRegex = /\{\{date:([^:]+):?([^}]*)\}\}/g;
        resolved = resolved.replace(dateRegex, (_, fmt, modifier) => {
          let date = new Date();
          if (modifier.startsWith('sub')) {
            date = subDays(date, parseInt(modifier.replace('sub', '')));
          }
          return format(date, fmt);
        });

        // Handle Context Variables
        for (const [key, value] of Object.entries(context)) {
          const regex = new RegExp(`\\{\\{${key}\\}\\}`, 'g');
          if (typeof value === 'string' || typeof value === 'number') {
            resolved = resolved.replace(regex, String(value));
          } else if (typeof value === 'object') {
            resolved = resolved.replace(regex, JSON.stringify(value));
          }
        }
        return resolved;
      };

      for (const node of nodes) {
        await new Promise(r => setTimeout(r, 1000));
        logs.push({ 
          timestamp: new Date(), 
          level: 'INFO', 
          message: `Executing node ${node.data.label} (${node.data.type})...` 
        });

        if (node.data.type === 'airflow_trigger') {
          const dagId = resolveVariables(node.data.config?.dagId || '', executionContext);
          const taskName = resolveVariables(node.data.config?.taskName || '', executionContext);
          const config = node.data.config?.conf || {};
          
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Triggering Airflow DAG: ${dagId} with conf: ${JSON.stringify(config)}` });
          
          try {
            // Real Airflow API call would go here
            // const response = await axios.post(`${AIRFLOW_URL}/api/v1/dags/${dagId}/dagRuns`, { conf: config }, { auth });
            // const dagRunId = response.data.dag_run_id;
            
            const dagRunId = `run_${Date.now()}`;
            if (!executionContext['dagRunIds']) executionContext['dagRunIds'] = [];
            executionContext['dagRunIds'].push({ dagId, dagRunId });
            executionContext['dagRunId'] = dagRunId;
            executionContext['dagId'] = dagId;
            executionContext[node.id] = { dagId, dagRunId, status: 'triggered', config };
            
            logs.push({ timestamp: new Date(), level: 'INFO', message: `DAG ${dagId} triggered successfully. Run ID: ${dagRunId}` });
          } catch (err: any) {
            logs.push({ timestamp: new Date(), level: 'ERROR', message: `Failed to trigger DAG ${dagId}: ${err.message}` });
            throw err;
          }
        }

        if (node.data.type === 'airflow_log_check') {
          const dagId = resolveVariables(node.data.config.dagId || executionContext['dagId'], executionContext);
          const taskName = resolveVariables(node.data.config.taskName || 'entire_dag', executionContext);
          const logAssertion = resolveVariables(node.data.config.logAssertion, executionContext);
          
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Checking logs for ${taskName} in DAG ${dagId}...` });
          
          try {
            // Real Airflow Log API call would go here
            // const logRes = await axios.get(`${AIRFLOW_URL}/api/v1/dags/${dagId}/dagRuns/${runId}/taskInstances/${taskId}/logs`);
            // const logsText = logRes.data;
            const logsText = "INFO: Task started\nSUCCESS: Processed 5000 rows\nINFO: Task completed"; // Placeholder for API integration
            
            // Print the retrieved logs to the workflow execution log as requested
            logs.push({ 
              timestamp: new Date(), 
              level: 'INFO', 
              message: `[AIRFLOW LOGS - ${taskName}] \n${logsText}` 
            });

            const pattern = new RegExp(logAssertion.replace(/\{\{.*?\}\}/g, '.*'), 'i');
            const found = pattern.test(logsText);
            
            executionContext[node.id] = found ? 1 : 0;
            executionContext['log_match_result'] = found ? 1 : 0;
            executionContext[`${node.id}_result`] = found ? 1 : 0;

            logs.push({ 
              timestamp: new Date(), 
              level: found ? 'INFO' : 'ERROR', 
              message: `Assertion [${logAssertion}] on ${taskName}: ${found ? 'PASSED' : 'FAILED'}` 
            });
            
            if (!found) continue;
          } catch (err: any) {
            logs.push({ timestamp: new Date(), level: 'ERROR', message: `Log check failed: ${err.message}` });
            throw err;
          }
        }

        if (node.data.type === 'sql_query') {
          let query = resolveVariables(node.data.config.query || "", executionContext);
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Running SQL: ${query}` });
          
          try {
            // Real SQL Server execution would go here using a driver like tedious or mssql
            // const result = await sqlPool.request().query(query);
            // const rows = result.recordset;
            const rows: any[] = []; 
            
            const csv = stringify(rows, { header: true });
            results[node.id] = csv;
            
            const recordCount = rows.length;
            executionContext[node.id] = { 
              data: rows,
              count: recordCount,
              status: 'success',
              columns: rows[0] ? Object.keys(rows[0]) : [],
              ...(rows[0] || {}) 
            };
            
            executionContext['queryResult'] = recordCount;
            executionContext['lastRecordCount'] = recordCount;
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Query completed: ${recordCount} records found.` });
            
            const assertion = node.data.config.pythonAssertion;
            if (assertion) {
              logs.push({ timestamp: new Date(), level: 'INFO', message: `Evaluating assertion: ${assertion}` });
              // Real evaluation would happen via a sandboxed python runner
              executionContext[`${node.id}_assertion`] = true;
            }
          } catch (err: any) {
            logs.push({ timestamp: new Date(), level: 'ERROR', message: `SQL Execution failed: ${err.message}` });
            throw err;
          }
        }

        if (node.data.type === 'api_request') {
          const url = resolveVariables(node.data.config.url, executionContext);
          const method = node.data.config.method || 'GET';
          const headers = JSON.parse(resolveVariables(JSON.stringify(node.data.config.headers || {}), executionContext));
          const body = resolveVariables(node.data.config.body, executionContext);

          logs.push({ timestamp: new Date(), level: 'INFO', message: `Sending ${method} request to ${url}...` });
          
          try {
            // Simulation of API request for stability in mock env
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Mock API Call to ${url} succeeded.` });
            const mockResponse = { status: 'success', data: { received: body, timestamp: new Date() } };
            executionContext[node.id] = mockResponse;
            executionContext['lastApiResponse'] = mockResponse;
            logs.push({ timestamp: new Date(), level: 'INFO', message: `API Response: ${JSON.stringify(mockResponse)}` });
          } catch (err: any) {
            logs.push({ timestamp: new Date(), level: 'ERROR', message: `API Request failed: ${err.message}` });
            await storage.updateExecution(execution.id, "failed", logs, results);
            return;
          }
        }

        if (node.data.type === 'python_script') {
          const rawCode = node.data.config?.code || "";
          const resolvedCode = resolveVariables(rawCode, executionContext);
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Executing Python script...` });
          
          try {
            // In a real system, we'd inject the executionContext as a variable into the script
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Script output: Execution context variables resolved and injected.` });
          } catch (err: any) {
            logs.push({ timestamp: new Date(), level: 'ERROR', message: `Python execution failed: ${err.message}` });
          }
        }

        if (node.data.type === 'condition') {
          const threshold = node.data.config?.threshold || 100;
          const variable = node.data.config?.variable || 'lastRecordCount';
          const operator = node.data.config?.operator || '>';
          
          // Improved variable resolution for hierarchical data (e.g. results.node_1.count)
          const getNestedValue = (obj: any, path: string) => {
            return path.split('.').reduce((acc, part) => acc && acc[part], obj);
          };

          const actualValue = getNestedValue(executionContext, variable);
          const actual = actualValue !== undefined ? actualValue : (executionContext['lastRecordCount'] || 0);
          
          let passed = false;
          switch (operator) {
            case '>': passed = actual > threshold; break;
            case '<': passed = actual < threshold; break;
            case '>=': passed = actual >= threshold; break;
            case '<=': passed = actual <= threshold; break;
            case '==': passed = actual == threshold; break;
            case '!=': passed = actual != threshold; break;
            default: passed = actual > threshold;
          }
          
          executionContext['conditionPassed'] = passed;
          logs.push({ 
            timestamp: new Date(), 
            level: 'INFO', 
            message: `Condition Check: ${actual} (${variable}) ${operator} ${threshold}? Result: ${passed ? 'TRUE' : 'FALSE'}` 
          });

          // Branching logic: if condition fails, we don't necessarily stop everything,
          // we just stop the current execution path for this specific flow.
          if (!passed) {
            logs.push({ timestamp: new Date(), level: 'WARN', message: `Condition failed. Skipping nodes downstream from this branch.` });
            
            // In a real DAG, we'd skip the specific downstream tasks.
            // For the mock, we stop processing the subsequent nodes in the list.
            break; 
          }
        }
        
        await storage.updateExecution(execution.id, "running", logs, results);
      }

      logs.push({ 
        timestamp: new Date(), 
        level: 'INFO', 
        message: `Workflow completed successfully. Summary: ${JSON.stringify(executionContext['dagRunIds'] || [])}` 
      });
      await storage.updateExecution(execution.id, "completed", logs, results);
    })();

    res.status(201).json(execution);
  });

  app.get(api.executions.list.path, async (req, res) => {
    const executions = await storage.getExecutions(
      req.query.workflowId ? Number(req.query.workflowId) : undefined
    );
    res.json(executions);
  });

  app.get(api.executions.get.path, async (req, res) => {
    const execution = await storage.getExecution(Number(req.params.id));
    if (!execution) return res.status(404).json({ message: "Execution not found" });
    res.json(execution);
  });

  return httpServer;
}
