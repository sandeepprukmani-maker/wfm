import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl } from "@shared/routes";
import { type InsertWorkflow } from "@shared/schema";
import { useToast } from "@/hooks/use-toast";

export function useWorkflows() {
  return useQuery({
    queryKey: [api.workflows.list.path],
    queryFn: async () => {
      const res = await fetch(api.workflows.list.path, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch workflows");
      return api.workflows.list.responses[200].parse(await res.json());
    },
  });
}

export function useWorkflow(id: number | null) {
  return useQuery({
    queryKey: [api.workflows.get.path, id],
    enabled: !!id,
    queryFn: async () => {
      if (!id) throw new Error("ID required");
      const url = buildUrl(api.workflows.get.path, { id });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to fetch workflow");
      return api.workflows.get.responses[200].parse(await res.json());
    },
  });
}

export function useCreateWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  
  return useMutation({
    mutationFn: async (data: InsertWorkflow) => {
      const res = await fetch(api.workflows.create.path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
        credentials: "include",
      });
      if (!res.ok) {
        if (res.status === 400) {
          const error = await res.json();
          throw new Error(error.message || "Validation failed");
        }
        throw new Error("Failed to create workflow");
      }
      return api.workflows.create.responses[201].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      toast({ title: "Success", description: "Workflow created successfully" });
    },
    onError: (err) => {
      toast({ variant: "destructive", title: "Error", description: err.message });
    }
  });
}

export function useUpdateWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async ({ id, ...updates }: { id: number } & Partial<InsertWorkflow>) => {
      const url = buildUrl(api.workflows.update.path, { id });
      const res = await fetch(url, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(updates),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to update workflow");
      return api.workflows.update.responses[200].parse(await res.json());
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      queryClient.invalidateQueries({ queryKey: [api.workflows.get.path, data.id] });
      toast({ title: "Saved", description: "Workflow changes saved" });
    },
    onError: (err) => {
      toast({ variant: "destructive", title: "Error", description: err.message });
    }
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
      toast({ title: "Deleted", description: "Workflow deleted" });
    },
  });
}

export function useGenerateWorkflow() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (data: { prompt: string, workflowId?: number }) => {
      const res = await fetch("/api/workflows/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to generate workflow");
      return (await res.json());
    },
    onSuccess: (_, variables) => {
      if (variables.workflowId) {
        queryClient.invalidateQueries({ queryKey: [api.workflows.get.path, variables.workflowId] });
      }
    }
  });
}

export function useExecuteWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (id: number) => {
      const url = buildUrl(api.workflows.execute.path, { id });
      const res = await fetch(url, { method: "POST", credentials: "include" });
      if (!res.ok) throw new Error("Failed to execute workflow");
      return api.workflows.execute.responses[201].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.executions.list.path] });
      toast({ title: "Started", description: "Workflow execution started" });
    },
    onError: (err) => {
      toast({ variant: "destructive", title: "Error", description: err.message });
    }
  });
}
