import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl } from "@shared/routes";
import { 
  type InsertWorkflow, 
  type GenerateWorkflowRequest,
  type Workflow,
  type Execution
} from "@shared/schema";
import { useToast } from "@/hooks/use-toast";

// === WORKFLOWS ===

export function useWorkflows() {
  return useQuery({
    queryKey: [api.workflows.list.path],
    queryFn: async () => {
      const res = await fetch(api.workflows.list.path);
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
      const res = await fetch(url);
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
    mutationFn: async (data: InsertWorkflow) => {
      const res = await fetch(api.workflows.create.path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to create workflow");
      return api.workflows.create.responses[201].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      toast({ title: "Workflow created", description: "Your new workflow is ready." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to create workflow.", variant: "destructive" });
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
      });
      if (!res.ok) throw new Error("Failed to update workflow");
      return api.workflows.update.responses[200].parse(await res.json());
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      queryClient.invalidateQueries({ queryKey: [api.workflows.get.path, data.id] });
      toast({ title: "Saved", description: "Workflow saved successfully." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to save workflow.", variant: "destructive" });
    }
  });
}

export function useDeleteWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (id: number) => {
      const url = buildUrl(api.workflows.delete.path, { id });
      const res = await fetch(url, { method: "DELETE" });
      if (!res.ok) throw new Error("Failed to delete workflow");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.workflows.list.path] });
      toast({ title: "Deleted", description: "Workflow has been removed." });
    },
  });
}

// === AI GENERATION ===

export function useGenerateWorkflow() {
  return useMutation({
    mutationFn: async (data: GenerateWorkflowRequest) => {
      const res = await fetch(api.workflows.generate.path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error("Failed to generate workflow");
      return api.workflows.generate.responses[200].parse(await res.json());
    },
  });
}

// === EXECUTIONS ===

export function useExecuteWorkflow() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (id: number) => {
      const url = buildUrl(api.workflows.execute.path, { id });
      const res = await fetch(url, { method: "POST" });
      if (!res.ok) throw new Error("Failed to execute workflow");
      return api.workflows.execute.responses[200].parse(await res.json());
    },
    onSuccess: (data) => {
      // Invalidate the executions list for the specific workflow
      queryClient.invalidateQueries({ 
        queryKey: [api.executions.list.path.replace(':id', String(data.workflowId))] 
      });
      toast({ title: "Execution started", description: "Workflow is running." });
    },
    onError: () => {
      toast({ title: "Error", description: "Failed to start execution.", variant: "destructive" });
    }
  });
}

export function useWorkflowExecutions(workflowId: number) {
  return useQuery({
    queryKey: [api.executions.list.path.replace(':id', String(workflowId))],
    queryFn: async () => {
      const url = buildUrl(api.executions.list.path, { id: workflowId });
      const res = await fetch(url);
      if (!res.ok) throw new Error("Failed to fetch executions");
      return api.executions.list.responses[200].parse(await res.json());
    },
    enabled: !!workflowId,
    refetchInterval: 2000, // Poll every 2s for more active updates
  });
}
