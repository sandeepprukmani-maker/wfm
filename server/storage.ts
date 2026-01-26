import { 
  workflows, credentials, executions,
  type Workflow, type InsertWorkflow,
  type Credential, type InsertCredential,
  type Execution
} from "@shared/schema";
import { db } from "./db";
import { eq, desc } from "drizzle-orm";

export interface IStorage {
  // Workflows
  getWorkflows(): Promise<Workflow[]>;
  getWorkflow(id: number): Promise<Workflow | undefined>;
  createWorkflow(workflow: InsertWorkflow): Promise<Workflow>;
  updateWorkflow(id: number, workflow: Partial<InsertWorkflow>): Promise<Workflow | undefined>;
  deleteWorkflow(id: number): Promise<void>;

  // Credentials
  getCredentials(): Promise<Credential[]>;
  getCredential(id: number): Promise<Credential | undefined>;
  createCredential(credential: InsertCredential): Promise<Credential>;
  deleteCredential(id: number): Promise<void>;

  // Executions
  getExecutions(workflowId?: number): Promise<Execution[]>;
  getExecution(id: number): Promise<Execution | undefined>;
  createExecution(workflowId: number): Promise<Execution>;
  updateExecution(id: number, status: string, logs: any[], results?: Record<string, string>): Promise<Execution>;
}

export class DatabaseStorage implements IStorage {
  // Workflow methods
  async getWorkflows(): Promise<Workflow[]> {
    return await db.select().from(workflows).orderBy(desc(workflows.updatedAt));
  }

  async getWorkflow(id: number): Promise<Workflow | undefined> {
    const [workflow] = await db.select().from(workflows).where(eq(workflows.id, id));
    return workflow;
  }

  async createWorkflow(insertWorkflow: InsertWorkflow): Promise<Workflow> {
    const [workflow] = await db.insert(workflows).values(insertWorkflow).returning();
    return workflow;
  }

  async updateWorkflow(id: number, updates: Partial<InsertWorkflow>): Promise<Workflow | undefined> {
    const [updated] = await db
      .update(workflows)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(workflows.id, id))
      .returning();
    return updated;
  }

  async deleteWorkflow(id: number): Promise<void> {
    await db.delete(workflows).where(eq(workflows.id, id));
  }

  // Credential methods
  async getCredentials(): Promise<Credential[]> {
    return await db.select().from(credentials).orderBy(desc(credentials.createdAt));
  }

  async getCredential(id: number): Promise<Credential | undefined> {
    const [credential] = await db.select().from(credentials).where(eq(credentials.id, id));
    return credential;
  }

  async createCredential(insertCredential: InsertCredential): Promise<Credential> {
    const [credential] = await db.insert(credentials).values(insertCredential).returning();
    return credential;
  }

  async deleteCredential(id: number): Promise<void> {
    await db.delete(credentials).where(eq(credentials.id, id));
  }

  // Execution methods
  async getExecutions(workflowId?: number): Promise<Execution[]> {
    let results;
    if (workflowId) {
      results = await db.select().from(executions).where(eq(executions.workflowId, workflowId)).orderBy(desc(executions.startedAt));
    } else {
      results = await db.select().from(executions).orderBy(desc(executions.startedAt));
    }
    
    return results.map(exec => ({
      ...exec,
      logs: typeof exec.logs === 'string' ? JSON.parse(exec.logs) : exec.logs
    }));
  }

  async getExecution(id: number): Promise<Execution | undefined> {
    const [execution] = await db.select().from(executions).where(eq(executions.id, id));
    if (!execution) return undefined;
    
    return {
      ...execution,
      logs: typeof execution.logs === 'string' ? JSON.parse(execution.logs) : execution.logs
    };
  }

  async createExecution(workflowId: number): Promise<Execution> {
    const [execution] = await db.insert(executions).values({
      workflowId,
      status: "pending",
      logs: JSON.stringify([]),
      startedAt: new Date(),
    }).returning();
    return execution;
  }

  async updateExecution(id: number, status: string, logs: any[], results?: Record<string, string>): Promise<Execution> {
    const updates: any = { status, logs: JSON.stringify(logs) };
    if (results) {
      updates.results = JSON.stringify(results);
    }
    if (status === "completed" || status === "failed") {
      updates.completedAt = new Date();
    }
    const [execution] = await db
      .update(executions)
      .set(updates)
      .where(eq(executions.id, id))
      .returning();
    
    return {
      ...execution,
      logs: typeof execution.logs === 'string' ? JSON.parse(execution.logs) : execution.logs,
      results: typeof execution.results === 'string' ? JSON.parse(execution.results) : execution.results
    };
  }
}

export const storage = new DatabaseStorage();
