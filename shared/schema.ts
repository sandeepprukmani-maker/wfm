import { pgTable, text, serial, integer, boolean, timestamp, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";
import { users } from "./models/auth";

// Re-export auth models so they are available
export * from "./models/auth";
export * from "./models/chat";

// === TABLE DEFINITIONS ===

export const workflows = pgTable("workflows", {
  id: serial("id").primaryKey(),
  userId: text("user_id").references(() => users.id).notNull(), // Owner of the workflow
  name: text("name").notNull(),
  description: text("description"),
  definition: text("definition").notNull(), // JSON/YAML string of the workflow definition
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const executions = pgTable("executions", {
  id: serial("id").primaryKey(),
  workflowId: integer("workflow_id").references(() => workflows.id).notNull(),
  status: text("status").notNull().default("pending"), // pending, running, completed, failed
  startedAt: timestamp("started_at").defaultNow(),
  completedAt: timestamp("completed_at"),
  logs: jsonb("logs").$type<ExecutionLog[]>(), // Structured logs
  outputs: jsonb("outputs"), // Final outputs of the workflow
  stepStatus: jsonb("step_status").$type<Record<string, StepStatus>>(), // Map of stepId -> status
});

// === TYPES ===

export type ExecutionLog = {
  timestamp: string;
  level: "info" | "warn" | "error";
  stepId?: string;
  message: string;
  details?: any;
};

export type StepStatus = {
  status: "pending" | "running" | "completed" | "failed" | "skipped";
  output?: any;
  error?: string;
  startTime?: string;
  endTime?: string;
};

export const insertWorkflowSchema = createInsertSchema(workflows).omit({ 
  id: true, 
  userId: true, // Set from session
  createdAt: true, 
  updatedAt: true 
});

// === EXPLICIT API CONTRACT TYPES ===

export type Workflow = typeof workflows.$inferSelect;
export type InsertWorkflow = z.infer<typeof insertWorkflowSchema>;

export type Execution = typeof executions.$inferSelect;

export type CreateWorkflowRequest = InsertWorkflow;
export type UpdateWorkflowRequest = Partial<InsertWorkflow>;

export type WorkflowResponse = Workflow;
export type ExecutionResponse = Execution;

export type ExecutionLogResponse = ExecutionLog;

// For executing a workflow
export type RunWorkflowRequest = {
  workflowId: number;
  params?: Record<string, any>; // Optional parameters for execution
};

// WebSocket events for real-time execution updates
export const WS_EVENTS = {
  EXECUTION_UPDATE: 'execution-update', // Status change, new logs, etc.
} as const;

export interface ExecutionUpdatePayload {
  executionId: number;
  status?: string;
  newLogs?: ExecutionLog[];
  stepUpdates?: Record<string, StepStatus>;
}
