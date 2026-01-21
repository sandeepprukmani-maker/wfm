import { db } from "./db";
import { 
  workflows, executions, 
  type InsertWorkflow, type UpdateWorkflowRequest, 
  type Workflow, type Execution 
} from "@shared/schema";
import { eq, desc } from "drizzle-orm";
import { authStorage } from "./replit_integrations/auth/storage";

export interface IStorage {
  // Workflows
  getWorkflow(id: number): Promise<Workflow | undefined>;
  getWorkflowsByUser(userId: string): Promise<Workflow[]>;
  createWorkflow(workflow: InsertWorkflow): Promise<Workflow>;
  updateWorkflow(id: number, updates: UpdateWorkflowRequest): Promise<Workflow>;
  deleteWorkflow(id: number): Promise<void>;

  // Executions
  createExecution(workflowId: number): Promise<Execution>;
  getExecution(id: number): Promise<Execution | undefined>;
  getExecutionsByWorkflow(workflowId: number): Promise<Execution[]>;
  updateExecution(id: number, updates: Partial<Execution>): Promise<Execution>;
}

export class DatabaseStorage implements IStorage {
  // Workflows
  async getWorkflow(id: number): Promise<Workflow | undefined> {
    const [workflow] = await db.select().from(workflows).where(eq(workflows.id, id));
    return workflow;
  }

  async getWorkflowsByUser(userId: string): Promise<Workflow[]> {
    return db.select().from(workflows)
      .where(eq(workflows.userId, userId))
      .orderBy(desc(workflows.updatedAt));
  }

  async createWorkflow(workflow: InsertWorkflow): Promise<Workflow> {
    const [newWorkflow] = await db.insert(workflows).values(workflow).returning();
    return newWorkflow;
  }

  async updateWorkflow(id: number, updates: UpdateWorkflowRequest): Promise<Workflow> {
    const [updated] = await db.update(workflows)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(workflows.id, id))
      .returning();
    return updated;
  }

  async deleteWorkflow(id: number): Promise<void> {
    await db.delete(workflows).where(eq(workflows.id, id));
  }

  // Executions
  async createExecution(workflowId: number): Promise<Execution> {
    const [execution] = await db.insert(executions).values({
      workflowId,
      status: "pending",
      logs: [],
      stepStatus: {}
    }).returning();
    return execution;
  }

  async getExecution(id: number): Promise<Execution | undefined> {
    const [execution] = await db.select().from(executions).where(eq(executions.id, id));
    return execution;
  }

  async getExecutionsByWorkflow(workflowId: number): Promise<Execution[]> {
    return db.select().from(executions)
      .where(eq(executions.workflowId, workflowId))
      .orderBy(desc(executions.startedAt));
  }

  async updateExecution(id: number, updates: Partial<Execution>): Promise<Execution> {
    const [updated] = await db.update(executions)
      .set(updates)
      .where(eq(executions.id, id))
      .returning();
    return updated;
  }
}

export const storage = new DatabaseStorage();
