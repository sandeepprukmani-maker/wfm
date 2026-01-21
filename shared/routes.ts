import { z } from 'zod';
import { insertWorkflowSchema, workflows, executions } from './schema';

export const errorSchemas = {
  validation: z.object({
    message: z.string(),
    field: z.string().optional(),
  }),
  notFound: z.object({
    message: z.string(),
  }),
  internal: z.object({
    message: z.string(),
  }),
  unauthorized: z.object({
    message: z.string(),
  }),
};

export const api = {
  workflows: {
    list: {
      method: 'GET' as const,
      path: '/api/workflows',
      responses: {
        200: z.array(z.custom<typeof workflows.$inferSelect>()),
        401: errorSchemas.unauthorized,
      },
    },
    get: {
      method: 'GET' as const,
      path: '/api/workflows/:id',
      responses: {
        200: z.custom<typeof workflows.$inferSelect>(),
        404: errorSchemas.notFound,
        401: errorSchemas.unauthorized,
      },
    },
    create: {
      method: 'POST' as const,
      path: '/api/workflows',
      input: insertWorkflowSchema,
      responses: {
        201: z.custom<typeof workflows.$inferSelect>(),
        400: errorSchemas.validation,
        401: errorSchemas.unauthorized,
      },
    },
    update: {
      method: 'PUT' as const,
      path: '/api/workflows/:id',
      input: insertWorkflowSchema.partial(),
      responses: {
        200: z.custom<typeof workflows.$inferSelect>(),
        400: errorSchemas.validation,
        404: errorSchemas.notFound,
        401: errorSchemas.unauthorized,
      },
    },
    delete: {
      method: 'DELETE' as const,
      path: '/api/workflows/:id',
      responses: {
        204: z.void(),
        404: errorSchemas.notFound,
        401: errorSchemas.unauthorized,
      },
    },
    run: {
      method: 'POST' as const,
      path: '/api/workflows/:id/run',
      input: z.object({
        params: z.record(z.any()).optional(),
      }).optional(),
      responses: {
        201: z.custom<typeof executions.$inferSelect>(), // Returns the created execution
        404: errorSchemas.notFound,
        401: errorSchemas.unauthorized,
      },
    },
  },
  executions: {
    list: {
      method: 'GET' as const,
      path: '/api/executions', // Can filter by workflowId
      input: z.object({
        workflowId: z.string().optional(),
      }).optional(),
      responses: {
        200: z.array(z.custom<typeof executions.$inferSelect>()),
        401: errorSchemas.unauthorized,
      },
    },
    get: {
      method: 'GET' as const,
      path: '/api/executions/:id',
      responses: {
        200: z.custom<typeof executions.$inferSelect>(),
        404: errorSchemas.notFound,
        401: errorSchemas.unauthorized,
      },
    },
  }
};

export function buildUrl(path: string, params?: Record<string, string | number>): string {
  let url = path;
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (url.includes(`:${key}`)) {
        url = url.replace(`:${key}`, String(value));
      }
    });
  }
  return url;
}

// Type Helpers
export type WorkflowInput = z.infer<typeof api.workflows.create.input>;
export type WorkflowResponse = z.infer<typeof api.workflows.create.responses[201]>;
export type ExecutionResponse = z.infer<typeof api.executions.get.responses[200]>;
