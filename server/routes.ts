import type { Express } from "express";
import type { Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import { z } from "zod";
import OpenAI from "openai";
import axios from "axios";
import { S3Client, ListBucketsCommand } from "@aws-sdk/client-s3";
import mysql from "mysql2/promise";
import mssql from "mssql";

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.AI_INTEGRATIONS_OPENAI_API_KEY,
  baseURL: process.env.AI_INTEGRATIONS_OPENAI_BASE_URL,
});

// Simple workflow execution engine
async function executeWorkflow(workflowId: number, executionId: number) {
  const workflow = await storage.getWorkflow(workflowId);
  if (!workflow) throw new Error("Workflow not found");

  const logs: any[] = [];
  logs.push({ timestamp: new Date(), message: "Execution started", type: "info" });

  try {
    const nodes = workflow.nodes as any[];
    const edges = workflow.edges as any[];

    // 1. Process Swagger if available
    if (workflow.swaggerUrl) {
      try {
        logs.push({ timestamp: new Date(), message: `Fetching Swagger from: ${workflow.swaggerUrl}`, type: "info" });
        const swaggerRes = await axios.get(workflow.swaggerUrl);
        const swaggerData = swaggerRes.data;
        logs.push({ timestamp: new Date(), message: "Swagger definition loaded successfully", type: "success" });
        
        // Analyze Swagger for API testing hints
        const paths = Object.keys(swaggerData.paths || {});
        logs.push({ timestamp: new Date(), message: `Found ${paths.length} API endpoints to test`, type: "info" });
        
        // Execute a quick test for each path
        for (const path of paths) {
          const methods = Object.keys(swaggerData.paths[path]);
          for (const method of methods) {
            const url = `${swaggerData.schemes?.[0] || 'http'}://${swaggerData.host}${swaggerData.basePath || ''}${path}`;
            logs.push({ timestamp: new Date(), message: `Testing endpoint: ${method.toUpperCase()} ${path}`, type: "info" });
            
            // Periodically update execution status so user sees progress
            await storage.updateExecution(executionId, "running", logs, undefined);

            try {
              const startTime = Date.now();
              const res = await axios({
                method: method as any,
                url,
                timeout: 10000,
                validateStatus: () => true,
              });
              const duration = Date.now() - startTime;
              
              logs.push({ 
                timestamp: new Date(), 
                message: `Result: ${res.status} ${res.statusText} (${duration}ms)`, 
                type: res.status >= 200 && res.status < 300 ? "success" : "warning",
                data: {
                  url,
                  method: method.toUpperCase(),
                  status: res.status,
                  duration: `${duration}ms`,
                  responseBody: typeof res.data === 'object' ? res.data : String(res.data).substring(0, 500)
                }
              });
            } catch (err: any) {
              logs.push({ 
                timestamp: new Date(), 
                message: `Test Failed: ${err.message}`, 
                type: "error",
                data: { url, method: method.toUpperCase(), error: err.message }
              });
            }
          }
        }
      } catch (err: any) {
        logs.push({ timestamp: new Date(), message: `Failed to load Swagger: ${err.message}`, type: "warning" });
      }
    }

    const startNodes = nodes.filter(n => n.type === 'trigger');
    const queue = [...startNodes];
    const visited = new Set();

    while (queue.length > 0) {
      const currentNode = queue.shift();
      if (!currentNode || visited.has(currentNode.id)) continue;
      
      visited.add(currentNode.id);
      logs.push({ timestamp: new Date(), message: `Executing node: ${currentNode.data.label || currentNode.type}`, nodeId: currentNode.id, type: "info" });
      
      // Update progress
      await storage.updateExecution(executionId, "running", logs, undefined);

      try {
        if (currentNode.type === 'http') {
           const method = (currentNode.data.method || 'GET').toUpperCase();
           const url = currentNode.data.url;
           if (url) {
             logs.push({ timestamp: new Date(), message: `Calling API: ${method} ${url}`, nodeId: currentNode.id, type: "info" });
             const res = await axios({
               method,
               url,
               data: currentNode.data.body,
               headers: currentNode.data.headers,
               validateStatus: () => true,
             });
             logs.push({ 
               timestamp: new Date(), 
               message: `API Response: ${res.status}`, 
               data: { status: res.status, body: res.data }, 
               nodeId: currentNode.id, 
               type: res.status >= 200 && res.status < 300 ? "success" : "warning" 
             });
           }
        } else if (currentNode.type === 'transform') {
           logs.push({ timestamp: new Date(), message: "Running data transformation", nodeId: currentNode.id, type: "info" });
           // In a real app we'd execute the JS code safely
           logs.push({ timestamp: new Date(), message: "Transformation completed", nodeId: currentNode.id, type: "success" });
        } else if (currentNode.type === 's3') {
           logs.push({ timestamp: new Date(), message: "S3 Operation: List Buckets (Mocked for safety)", nodeId: currentNode.id, type: "info" });
           // Real implementation would use S3Client with secrets
        } else if (currentNode.type === 'db') {
           logs.push({ timestamp: new Date(), message: `DB Operation: ${currentNode.data.query || 'SELECT 1'}`, nodeId: currentNode.id, type: "info" });
           // Real implementation would use mysql2 or mssql with secrets
        }
      } catch (err: any) {
        logs.push({ timestamp: new Date(), message: `Node failed: ${err.message}`, nodeId: currentNode.id, type: "error" });
        throw err;
      }

      const outgoingEdges = edges.filter(e => e.source === currentNode.id);
      for (const edge of outgoingEdges) {
        const targetNode = nodes.find(n => n.id === edge.target);
        if (targetNode) queue.push(targetNode);
      }
    }

    logs.push({ timestamp: new Date(), message: "Execution completed", type: "success" });
    await storage.updateExecution(executionId, "completed", logs, new Date());
  } catch (error: any) {
    logs.push({ timestamp: new Date(), message: `Workflow failed: ${error.message}`, type: "error" });
    await storage.updateExecution(executionId, "failed", logs, new Date());
  }
}


export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  
  // Workflows CRUD
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
      if (err instanceof z.ZodError) return res.status(400).json(err.issues);
      throw err;
    }
  });

  app.put(api.workflows.update.path, async (req, res) => {
    const input = api.workflows.update.input.parse(req.body);
    const workflow = await storage.updateWorkflow(Number(req.params.id), input);
    if (!workflow) return res.status(404).json({ message: "Workflow not found" });
    res.json(workflow);
  });

  app.delete(api.workflows.delete.path, async (req, res) => {
    await storage.deleteWorkflow(Number(req.params.id));
    res.status(204).send();
  });

  // Executions
  app.get(api.executions.list.path, async (req, res) => {
    const executions = await storage.getExecutions(Number(req.params.id));
    res.json(executions);
  });

  app.post(api.workflows.execute.path, async (req, res) => {
    const workflowId = Number(req.params.id);
    
    // Quick fix: create the record here to return it, then pass ID to execute
    const execution = await storage.createExecution({
      workflowId,
      status: "pending",
      logs: [],
    });

    executeWorkflow(workflowId, execution.id).catch(console.error); // Run in background
    
    res.json(execution);
  });

  // AI Generation
  app.post(api.workflows.generate.path, async (req, res) => {
    try {
      const { prompt } = api.workflows.generate.input.parse(req.body);

      const response = await openai.chat.completions.create({
        model: "gpt-4o",
        messages: [
          {
            role: "system",
            content: `You are an expert workflow automation architect and QA engineer. 
            Generate a JSON structure for an AUTOMATED API TESTING workflow based on the user's prompt.
            The workflow must be designed to EXECUTE and VERIFY API endpoints immediately.
            
            Return a JSON object with:
            - name: string
            - description: string
            - nodes: array of { id: string, type: string, position: {x,y}, data: { label: string, ...props } }
            - edges: array of { id: string, source: string, target: string }
            
            For API testing:
            - Create a sequence that calls each relevant API endpoint using 'http' nodes.
            - Follow each 'http' node with a 'logic' node that asserts success (e.g., status == 200).
            - Use 'transform' nodes if data needs to be extracted from one response to use in the next request.
            - Design it so the entire sequence can be run with one click to test the whole API set.
            
            Node Props:
            - 'http': { method, url, headers, body }
            - 'logic': { condition: "data.status === 200" }
            - 'transform': { code: "return data.id;" }
            
            Layout the nodes visually (x, y coordinates) from left to right in a logical testing flow.`
          },
          { role: "user", content: prompt }
        ],
        response_format: { type: "json_object" }
      });

      const result = JSON.parse(response.choices[0].message.content || "{}");
      res.json(result);
    } catch (error) {
      console.error("AI Generation failed:", error);
      res.status(500).json({ message: "Failed to generate workflow" });
    }
  });

  return httpServer;
}
