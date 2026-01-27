import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl } from "@shared/routes";

export function useExecutions(workflowId?: number) {
  return useQuery({
    queryKey: [api.executions.list.path, { workflowId }],
    queryFn: async () => {
      let url = api.executions.list.path;
      if (workflowId) {
        url += `?workflowId=${workflowId}`;
      }
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch executions");
      return api.executions.list.responses[200].parse(await res.json());
    },
    refetchInterval: 5000, // Poll every 5s for status updates
  });
}

export function useDeleteExecutions(workflowId?: number) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async () => {
      let url = api.executions.list.path;
      if (workflowId) {
        url += `?workflowId=${workflowId}`;
      }
      const res = await fetch(url, { 
        method: "DELETE",
        credentials: "include" 
      });
      if (!res.ok) throw new Error("Failed to delete executions");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.executions.list.path] });
    },
  });
}

export function useExecution(id: number | null) {
  return useQuery({
    queryKey: [api.executions.get.path, id],
    enabled: !!id,
    queryFn: async () => {
      if (!id) throw new Error("ID required");
      const url = buildUrl(api.executions.get.path, { id });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 404) return null;
      if (!res.ok) throw new Error("Failed to fetch execution");
      return api.executions.get.responses[200].parse(await res.json());
    },
    refetchInterval: (query) => {
      // Poll faster if running, stop if completed/failed
      const data = query.state.data;
      if (data && (data.status === 'completed' || data.status === 'failed')) {
        return false;
      }
      return 1000;
    },
  });
}
