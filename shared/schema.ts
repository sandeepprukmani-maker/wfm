import { sqliteTable, text, integer, blob } from "drizzle-orm/sqlite-core";
import { relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// === WORKFLOW DEFINITIONS ===
export const workflows = sqliteTable("workflows", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name").notNull(),
  description: text("description"),
  nodes: text("nodes", { mode: 'json' }).notNull().default('[]'), // React Flow nodes
  edges: text("edges", { mode: 'json' }).notNull().default('[]'), // React Flow edges
  createdAt: integer("created_at", { mode: 'timestamp' }).defaultNow(),
  updatedAt: integer("updated_at", { mode: 'timestamp' }).defaultNow(),
});

// === CREDENTIALS ===
export const credentials = sqliteTable("credentials", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name").notNull(),
  type: text("type").notNull(), // 'airflow' | 'mssql'
  data: text("data", { mode: 'json' }).notNull(), // Encrypted or structured connection info
  createdAt: integer("created_at", { mode: 'timestamp' }).defaultNow(),
});

// === EXECUTIONS ===
export const executions = sqliteTable("executions", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  workflowId: integer("workflow_id").notNull(),
  status: text("status").notNull().default("pending"), // pending, running, completed, failed
  logs: text("logs", { mode: 'json' }).default('[]'), // Array of log entries
  startedAt: integer("started_at", { mode: 'timestamp' }).defaultNow(),
  completedAt: integer("completed_at", { mode: 'timestamp' }),
});

// === RELATIONS ===
export const workflowRelations = relations(workflows, ({ many }) => ({
  executions: many(executions),
}));

export const executionRelations = relations(executions, ({ one }) => ({
  workflow: one(workflows, {
    fields: [executions.workflowId],
    references: [workflows.id],
  }),
}));

// === ZOD SCHEMAS ===
export const insertWorkflowSchema = createInsertSchema(workflows).omit({ 
  id: true, 
  createdAt: true, 
  updatedAt: true 
});

export const insertCredentialSchema = createInsertSchema(credentials).omit({ 
  id: true, 
  createdAt: true 
});

// === TYPES ===
export type Workflow = typeof workflows.$inferSelect;
export type InsertWorkflow = z.infer<typeof insertWorkflowSchema>;
export type Credential = typeof credentials.$inferSelect;
export type InsertCredential = z.infer<typeof insertCredentialSchema>;
export type Execution = typeof executions.$inferSelect;

// Node types for the visual editor
export type WorkflowNodeType = 'airflow_trigger' | 'sql_query' | 'python_script';

export interface WorkflowNodeData {
  label: string;
  type: WorkflowNodeType;
  config: Record<string, any>;
}

// Prompt generation request
export interface GenerateWorkflowRequest {
  prompt: string;
}
