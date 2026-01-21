import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl, type WorkflowInput } from "@shared/routes";
import type { Workflow, Execution, ExecutionLog, StepStatus } from "@shared/schema";
import { useToast } from "@/hooks/use-toast";

// === Workflows ===

export function useWorkflows() {
  return useQuery({
    queryKey: [api.workflows.list.path],
    queryFn: async () => {
      const res = await fetch(api.workflows.list.path, { credentials: "include" });
      if (res.status === 401) return null;
      if (!res.ok) throw new Error("Failed to fetch workflows");
      return api.workflows.list.responses[200].parse(await res.json());
    },
  });
}

export function useWorkflow(id: number) {
  return useQuery({
    queryKey: [api.workflows.get.path, id],
    queryFn: async () => {
      const url = buildUrl(api.workflows.get.path, { id });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to fetch workflow");
      return api.workflows.get.responses[200].parse(await res.json());
    },
    enabled: !!id,
  });
}

export function useCreateWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  
  return useMutation({
    mutationFn: async (data: WorkflowInput) => {
      const res = await fetch(api.workflows.create.path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to create workflow");
      return api.workflows.create.responses[201].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      toast({ title: "Success", description: "Workflow created successfully" });
    },
    onError: (error) => {
      toast({ title: "Error", description: error.message, variant: "destructive" });
    },
  });
}

export function useUpdateWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async ({ id, ...data }: { id: number } & Partial<WorkflowInput>) => {
      const url = buildUrl(api.workflows.update.path, { id });
      const res = await fetch(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to update workflow");
      return api.workflows.update.responses[200].parse(await res.json());
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      queryClient.invalidateQueries({ queryKey: [api.workflows.get.path, data.id] });
      toast({ title: "Success", description: "Workflow saved successfully" });
    },
    onError: (error) => {
      toast({ title: "Error", description: error.message, variant: "destructive" });
    },
  });
}

export function useDeleteWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (id: number) => {
      const url = buildUrl(api.workflows.delete.path, { id });
      const res = await fetch(url, { method: "DELETE", credentials: "include" });
      if (!res.ok) throw new Error("Failed to delete workflow");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      toast({ title: "Success", description: "Workflow deleted" });
    },
  });
}

export function useRunWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async ({ id, params }: { id: number, params?: Record<string, any> }) => {
      const url = buildUrl(api.workflows.run.path, { id });
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ params }),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to start execution");
      return api.workflows.run.responses[201].parse(await res.json());
    },
    onSuccess: (data) => {
      toast({ title: "Started", description: `Execution #${data.id} started` });
      // Invalidate executions list
      queryClient.invalidateQueries({ queryKey: [api.executions.list.path] });
    },
  });
}

// === Executions ===

export function useExecutions(workflowId?: string) {
  return useQuery({
    queryKey: [api.executions.list.path, workflowId],
    queryFn: async () => {
      // Build query params
      const query = workflowId ? `?workflowId=${workflowId}` : '';
      const res = await fetch(api.executions.list.path + query, { credentials: "include" });
      if (res.status === 401) return null;
      if (!res.ok) throw new Error("Failed to fetch executions");
      return api.executions.list.responses[200].parse(await res.json());
    },
  });
}

export function useExecution(id: number) {
  return useQuery({
    queryKey: [api.executions.get.path, id],
    queryFn: async () => {
      const url = buildUrl(api.executions.get.path, { id });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to fetch execution");
      return api.executions.get.responses[200].parse(await res.json());
    },
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data && (data.status === 'pending' || data.status === 'running')) {
        return 1000; // Poll every 1s if running
      }
      return false;
    },
  });
}
