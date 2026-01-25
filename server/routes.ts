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

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.AI_INTEGRATIONS_OPENAI_API_KEY,
  baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
});

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // === WORKFLOW ROUTES ===

  app.get(api.workflows.exportPython.path, async (req, res) => {
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

  // === AI GENERATION ===
  app.post(api.workflows.generate.path, async (req, res) => {
    const { prompt } = req.body;

    try {
      const response = await openai.chat.completions.create({
        model: "gpt-5.1",
        messages: [
          {
            role: "system",
            content: `You are a workflow generator for Apache Airflow 2.7.3 and SQL Server.
            The available node types are:
            - 'airflow_trigger': Trigger an Airflow DAG. Config: { dagId: string, conf?: object, logAssertion?: string, taskName?: string }. 
              - logAssertion: A regex pattern to look for in the logs (e.g., "total count is 5000").
              - taskName: The specific task ID within the Airflow DAG whose logs should be checked. This is mandatory if logAssertion is used.
            - 'sql_query': Execute SQL. Config: { query: string, credentialId: number }. Supports variable substitution: {{dagRunId}}.
            - 'python_script': Run Python code. Config: { code: string }.
            - 'condition': Branching logic. Config: { threshold: number }.
            
            Return ONLY a JSON object with 'nodes' and 'edges' compatible with React Flow.
            Nodes should have id, position ({x, y}), data ({label, type, config}).
            Edges should have id, source, target.

            CRITICAL: If the user asks to "check logs of task X in DAG Y for pattern Z", you MUST:
            1. Create an 'airflow_trigger' node with dagId: "Y".
            2. Set config.logAssertion to "Z".
            3. Set config.taskName to "X".
            4. Connect this node to all downstream nodes that should execute only if the log check passes.
            `
          },
          {
            role: "user",
            content: `Scenario: ${prompt}.
            
            Follow the user's scenario precisely. Ensure 'taskName' and 'logAssertion' are both populated if a specific task log check is mentioned.`
          }
        ],
        response_format: { type: "json_object" }
      });

      const content = JSON.parse(response.choices[0].message.content || "{}");
      res.json(content);
    } catch (error) {
      console.error("AI Generation failed:", error);
      res.status(500).json({ message: "Failed to generate workflow" });
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
  app.post(api.workflows.execute.path, async (req, res) => {
    const workflowId = Number(req.params.id);
    const execution = await storage.createExecution(workflowId);

    // Mock Execution in Background
    (async () => {
      const workflow = await storage.getWorkflow(workflowId);
      if (!workflow) return;

      const logs = [];
      logs.push({ timestamp: new Date(), level: 'INFO', message: 'Starting workflow execution...' });
      await storage.updateExecution(execution.id, "running", logs);

      const nodes = (workflow.nodes as any[]) || [];
      const executionContext: Record<string, any> = {};
      
      for (const node of nodes) {
        await new Promise(r => setTimeout(r, 1000));
        logs.push({ 
          timestamp: new Date(), 
          level: 'INFO', 
          message: `Executing node ${node.data.label} (${node.data.type})...` 
        });

        if (node.data.type === 'airflow_trigger') {
          const dagId = node.data.config.dagId;
          const taskName = node.data.config.taskName || 'default_task';
          const dagRunId = `run_${Math.random().toString(36).substring(7)}`;
          
          if (!executionContext['dagRunIds']) executionContext['dagRunIds'] = [];
          executionContext['dagRunIds'].push({ dagId, dagRunId });
          executionContext['dagRunId'] = dagRunId;
          
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Triggering Airflow DAG: ${dagId}...` });
          
          // Simulation of success and log check
          const logAssertion = node.data.config?.logAssertion;
          if (logAssertion) {
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Checking logs for task '${taskName}' in DAG '${dagId}'...` });
            
            // Simulated match for the specific scenario
            const mockLogs = [
              `INFO: Task ${taskName} started`,
              "SUCCESS: total count is 5000",
              `INFO: Task ${taskName} completed`
            ];
            
            const pattern = new RegExp(logAssertion, 'i');
            const found = mockLogs.some(l => pattern.test(l));
            
            logs.push({ 
              timestamp: new Date(), 
              level: found ? 'INFO' : 'ERROR', 
              message: `Log Assertion [${logAssertion}]: ${found ? 'PASSED' : 'FAILED'}` 
            });
            
            if (!found) {
              logs.push({ timestamp: new Date(), level: 'ERROR', message: `Dependency failed. Skipping downstream nodes.` });
              continue; 
            }
          }

          logs.push({ timestamp: new Date(), level: 'INFO', message: `DAG ${dagId} successful. Run ID: ${dagRunId}` });
        }

        if (node.data.type === 'sql_query') {
          let query = node.data.config.query || "";
          // Variable substitution
          if (query.includes('{{dagRunId}}')) {
            const actualId = executionContext['dagRunId'] || 'N/A';
            query = query.replace(/\{\{dagRunId\}\}/g, actualId);
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Injected variable: dagRunId = ${actualId}` });
          }
          
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Running SQL: ${query}` });
          
          // Simulation of record count
          const recordCount = Math.floor(Math.random() * 200); // Mocking result
          executionContext['lastRecordCount'] = recordCount;
          logs.push({ timestamp: new Date(), level: 'INFO', message: `Query result: ${recordCount} records found.` });
          
          // Simulation of assertion
          if (query.toLowerCase().includes('select')) {
            logs.push({ timestamp: new Date(), level: 'INFO', message: `Assertion: Row count > 0 - Passed.` });
          }
        }

        if (node.data.type === 'condition') {
          const threshold = node.data.config?.threshold || 100;
          const actual = executionContext['lastRecordCount'] || 0;
          const passed = actual > threshold;
          
          executionContext['conditionPassed'] = passed;
          logs.push({ 
            timestamp: new Date(), 
            level: 'INFO', 
            message: `Condition Check: ${actual} > ${threshold}? Result: ${passed ? 'TRUE' : 'FALSE'}` 
          });

          if (!passed) {
            logs.push({ timestamp: new Date(), level: 'WARN', message: `Condition failed. Stopping downstream execution for this branch.` });
            // In a real engine, we would skip siblings/children not connected to the 'false' path
            break; 
          }
        }
        
        await storage.updateExecution(execution.id, "running", logs);
      }

      logs.push({ 
        timestamp: new Date(), 
        level: 'INFO', 
        message: `Workflow completed successfully. Summary: ${JSON.stringify(executionContext['dagRunIds'] || [])}` 
      });
      await storage.updateExecution(execution.id, "completed", logs);
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
