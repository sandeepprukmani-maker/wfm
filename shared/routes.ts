import { z } from 'zod';
import { insertWorkflowSchema, insertCredentialSchema, workflows, credentials, executions } from './schema';

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
      input: z.object({ prompt: z.string() }),
      responses: {
        200: z.object({
          nodes: z.array(z.any()),
          edges: z.array(z.any()),
        }),
      },
    },
    execute: {
      method: 'POST' as const,
      path: '/api/workflows/:id/execute',
      responses: {
        201: z.custom<typeof executions.$inferSelect>(),
        404: errorSchemas.notFound,
      },
    },
    exportPython: {
      method: 'GET' as const,
      path: '/api/workflows/:id/export',
      responses: {
        200: z.object({ code: z.string() }),
        404: errorSchemas.notFound,
      },
    }
  },
  credentials: {
    list: {
      method: 'GET' as const,
      path: '/api/credentials',
      responses: {
        200: z.array(z.custom<typeof credentials.$inferSelect>()),
      },
    },
    create: {
      method: 'POST' as const,
      path: '/api/credentials',
      input: insertCredentialSchema,
      responses: {
        201: z.custom<typeof credentials.$inferSelect>(),
        400: errorSchemas.validation,
      },
    },
    delete: {
      method: 'DELETE' as const,
      path: '/api/credentials/:id',
      responses: {
        204: z.void(),
        404: errorSchemas.notFound,
      },
    },
  },
  executions: {
    list: {
      method: 'GET' as const,
      path: '/api/executions',
      input: z.object({ workflowId: z.coerce.number().optional() }).optional(),
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
