import { storage } from "./storage";
import type { Execution, ExecutionLog, StepStatus } from "@shared/schema";
import YAML from "yaml";

// Simple in-memory engine for MVP
export async function executeWorkflow(executionId: number, definitionYaml: string) {
  console.log(`Starting execution ${executionId}`);
  
  try {
    // 1. Parse Definition
    const workflow = YAML.parse(definitionYaml);
    if (!workflow || !workflow.steps) {
        throw new Error("Invalid workflow definition: 'steps' missing");
    }

    await updateStatus(executionId, "running");
    await log(executionId, "info", "system", "Workflow execution started");

    // 2. Execute Steps (Sequential for MVP)
    const context: Record<string, any> = {};
    
    for (const step of workflow.steps) {
        await log(executionId, "info", step.id, `Starting step: ${step.type}`);
        await updateStepStatus(executionId, step.id, { status: "running", startTime: new Date().toISOString() });

        try {
            const result = await runStep(step, context);
            context[step.id] = result;
            
            await updateStepStatus(executionId, step.id, { 
                status: "completed", 
                output: result, 
                endTime: new Date().toISOString() 
            });
            await log(executionId, "info", step.id, `Step completed successfully`);
        } catch (error: any) {
            console.error(`Step ${step.id} failed:`, error);
            await updateStepStatus(executionId, step.id, { 
                status: "failed", 
                error: error.message, 
                endTime: new Date().toISOString() 
            });
            await log(executionId, "error", step.id, `Step failed: ${error.message}`);
            throw error; // Stop execution on failure
        }
    }

    await updateStatus(executionId, "completed", context);
    await log(executionId, "info", "system", "Workflow execution completed successfully");

  } catch (error: any) {
    console.error(`Execution ${executionId} failed:`, error);
    await updateStatus(executionId, "failed");
    await log(executionId, "error", "system", `Workflow execution failed: ${error.message}`);
  }
}

async function runStep(step: any, context: any) {
    // Simulate processing delay
    await new Promise(resolve => setTimeout(resolve, 1000));

    switch (step.type) {
        case "sql.execute":
            // Mock SQL execution
            return { rows: [{ count: 1500 }], affected: 0 };
        
        case "storage.check":
            // Mock Storage check
            if (step.path?.includes("fail")) throw new Error("File not found");
            return { exists: true, size: 1024 };

        case "assert.sql":
        case "assert.logic":
            // Simple expression evaluation (unsafe eval for MVP demo only)
            // Real impl should use a safe expression parser
            const condition = step.condition;
            // Replace {{ step.result }} variables
            const evalStr = condition.replace(/\{\{\s*(\w+)\.(\w+)\s*\}\}/g, (_: any, stepId: string, prop: string) => {
                return JSON.stringify(context[stepId]?.[prop]);
            });
            
            // Very unsafe, do not use in production!
            // eslint-disable-next-line no-eval
            const passed = eval(evalStr);
            if (!passed) throw new Error(`Assertion failed: ${condition}`);
            return { passed: true };

        default:
            return { message: "Executed generic step" };
    }
}

// Helpers

async function updateStatus(id: number, status: string, outputs?: any) {
    await storage.updateExecution(id, { 
        status, 
        completedAt: status === "completed" || status === "failed" ? new Date() : null,
        outputs
    });
}

async function updateStepStatus(executionId: number, stepId: string, status: StepStatus) {
    const execution = await storage.getExecution(executionId);
    if (!execution) return;

    const currentStepStatus = (execution.stepStatus as Record<string, StepStatus>) || {};
    currentStepStatus[stepId] = { ...currentStepStatus[stepId], ...status };

    await storage.updateExecution(executionId, { stepStatus: currentStepStatus });
}

async function log(executionId: number, level: "info" | "warn" | "error", stepId: string | undefined, message: string) {
    const execution = await storage.getExecution(executionId);
    if (!execution) return;

    const logs = (execution.logs as ExecutionLog[]) || [];
    logs.push({
        timestamp: new Date().toISOString(),
        level,
        stepId,
        message
    });

    await storage.updateExecution(executionId, { logs });
}
