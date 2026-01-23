import { db } from "./db";
import {
  workflows,
  executions,
  type Workflow,
  type InsertWorkflow,
  type Execution,
  type InsertExecution,
} from "@shared/schema";
import { eq, desc } from "drizzle-orm";

export interface IStorage {
  // Workflows
  getWorkflows(): Promise<Workflow[]>;
  getWorkflow(id: number): Promise<Workflow | undefined>;
  createWorkflow(workflow: InsertWorkflow): Promise<Workflow>;
  updateWorkflow(id: number, workflow: Partial<InsertWorkflow>): Promise<Workflow>;
  deleteWorkflow(id: number): Promise<void>;

  // Executions
  getExecutions(workflowId: number): Promise<Execution[]>;
  getExecution(id: number): Promise<Execution | undefined>;
  createExecution(execution: InsertExecution): Promise<Execution>;
  updateExecution(id: number, status: string, logs: any[], finishedAt?: Date): Promise<Execution>;
}

export class DatabaseStorage implements IStorage {
  async getWorkflows(): Promise<Workflow[]> {
    return await db.select().from(workflows).orderBy(desc(workflows.updatedAt));
  }

  async getWorkflow(id: number): Promise<Workflow | undefined> {
    const [workflow] = await db.select().from(workflows).where(eq(workflows.id, id));
    return workflow;
  }

  async createWorkflow(insertWorkflow: InsertWorkflow): Promise<Workflow> {
    const [workflow] = await db
      .insert(workflows)
      .values(insertWorkflow)
      .returning();
    return workflow;
  }

  async updateWorkflow(id: number, update: Partial<InsertWorkflow>): Promise<Workflow> {
    const [workflow] = await db
      .update(workflows)
      .set({ ...update, updatedAt: new Date() })
      .where(eq(workflows.id, id))
      .returning();
    return workflow;
  }

  async deleteWorkflow(id: number): Promise<void> {
    await db.delete(workflows).where(eq(workflows.id, id));
  }

  async getExecutions(workflowId: number): Promise<Execution[]> {
    return await db
      .select()
      .from(executions)
      .where(eq(executions.workflowId, workflowId))
      .orderBy(desc(executions.startedAt));
  }

  async getExecution(id: number): Promise<Execution | undefined> {
    const [execution] = await db.select().from(executions).where(eq(executions.id, id));
    return execution;
  }

  async createExecution(insertExecution: InsertExecution): Promise<Execution> {
    const [execution] = await db
      .insert(executions)
      .values(insertExecution)
      .returning();
    return execution;
  }

  async updateExecution(id: number, status: string, logs: any[], finishedAt?: Date): Promise<Execution> {
    const [execution] = await db
      .update(executions)
      .set({ status, logs, finishedAt })
      .where(eq(executions.id, id))
      .returning();
    return execution;
  }
}

export const storage = new DatabaseStorage();
