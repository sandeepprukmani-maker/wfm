import { pgTable, text, serial, timestamp, jsonb, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// === WORKFLOWS ===
export const workflows = pgTable("workflows", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  description: text("description"),
  swaggerUrl: text("swagger_url"), // Added for API testing
  nodes: jsonb("nodes").$type<any[]>().notNull().default([]),
  edges: jsonb("edges").$type<any[]>().notNull().default([]),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// === EXECUTIONS ===
export const executions = pgTable("executions", {
  id: serial("id").primaryKey(),
  workflowId: serial("workflow_id").references(() => workflows.id),
  status: text("status").notNull(), // 'pending', 'running', 'completed', 'failed'
  logs: jsonb("logs").$type<any[]>(), // Array of execution steps/logs
  startedAt: timestamp("started_at").defaultNow(),
  finishedAt: timestamp("finished_at"),
});

// === SCHEMAS ===
export const insertWorkflowSchema = createInsertSchema(workflows).omit({ 
  id: true, 
  createdAt: true, 
  updatedAt: true 
});

export const insertExecutionSchema = createInsertSchema(executions).omit({
  id: true,
  startedAt: true,
  finishedAt: true
});

// === TYPES ===
export type Workflow = typeof workflows.$inferSelect;
export type InsertWorkflow = z.infer<typeof insertWorkflowSchema>;
export type Execution = typeof executions.$inferSelect;
export type InsertExecution = z.infer<typeof insertExecutionSchema>;

// Request types
export type CreateWorkflowRequest = InsertWorkflow;
export type UpdateWorkflowRequest = Partial<InsertWorkflow>;

// AI Generation types
export const generateWorkflowSchema = z.object({
  prompt: z.string().min(1, "Prompt is required"),
});
export type GenerateWorkflowRequest = z.infer<typeof generateWorkflowSchema>;

// Node Types for validation (simplified)
export const nodeSchema = z.object({
  id: z.string(),
  type: z.string(), // 'trigger', 'http', 'logic', 'transform'
  position: z.object({ x: z.number(), y: z.number() }),
  data: z.record(z.any()),
});
