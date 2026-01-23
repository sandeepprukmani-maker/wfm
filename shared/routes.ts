import { z } from 'zod';
import { insertWorkflowSchema, insertExecutionSchema, generateWorkflowSchema, workflows, executions } from './schema';

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
};

export const api = {
  workflows: {
    list: {
      method: 'GET' as const,
      path: '/api/workflows',
      responses: {
        200: z.array(z.custom<typeof workflows.$inferSelect>()),
      },
    },
    get: {
      method: 'GET' as const,
      path: '/api/workflows/:id',
      responses: {
        200: z.custom<typeof workflows.$inferSelect>(),
        404: errorSchemas.notFound,
      },
    },
    create: {
      method: 'POST' as const,
      path: '/api/workflows',
      input: insertWorkflowSchema,
      responses: {
        201: z.custom<typeof workflows.$inferSelect>(),
        400: errorSchemas.validation,
      },
    },
    update: {
      method: 'PUT' as const,
      path: '/api/workflows/:id',
      input: insertWorkflowSchema.partial(),
      responses: {
        200: z.custom<typeof workflows.$inferSelect>(),
        404: errorSchemas.notFound,
      },
    },
    delete: {
      method: 'DELETE' as const,
      path: '/api/workflows/:id',
      responses: {
        204: z.void(),
        404: errorSchemas.notFound,
      },
    },
    generate: {
      method: 'POST' as const,
      path: '/api/workflows/generate',
      input: generateWorkflowSchema,
      responses: {
        200: z.object({
          nodes: z.array(z.any()),
          edges: z.array(z.any()),
          name: z.string().optional(),
          description: z.string().optional(),
        }),
        500: errorSchemas.internal,
      },
    },
    execute: {
      method: 'POST' as const,
      path: '/api/workflows/:id/execute',
      responses: {
        200: z.custom<typeof executions.$inferSelect>(), // Returns the execution record
        404: errorSchemas.notFound,
      },
    },
  },
  executions: {
    list: {
      method: 'GET' as const,
      path: '/api/workflows/:id/executions',
      responses: {
        200: z.array(z.custom<typeof executions.$inferSelect>()),
      },
    },
    get: {
      method: 'GET' as const,
      path: '/api/executions/:id',
      responses: {
        200: z.custom<typeof executions.$inferSelect>(),
        404: errorSchemas.notFound,
      },
    },
  },
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
