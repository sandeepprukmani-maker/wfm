import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl, type InsertCredential } from "@shared/routes";
import { useToast } from "@/hooks/use-toast";

export function useCredentials() {
  return useQuery({
    queryKey: [api.credentials.list.path],
    queryFn: async () => {
      const res = await fetch(api.credentials.list.path, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch credentials");
      return api.credentials.list.responses[200].parse(await res.json());
    },
  });
}

export function useCreateCredential() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  
  return useMutation({
    mutationFn: async (data: InsertCredential) => {
      const res = await fetch(api.credentials.create.path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to create credential");
      return api.credentials.create.responses[201].parse(await res.json());
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.credentials.list.path] });
      toast({ title: "Success", description: "Credential stored securely" });
    },
    onError: (err) => {
      toast({ variant: "destructive", title: "Error", description: err.message });
    }
  });
}

export function useDeleteCredential() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (id: number) => {
      const url = buildUrl(api.credentials.delete.path, { id });
      const res = await fetch(url, { method: "DELETE", credentials: "include" });
      if (!res.ok) throw new Error("Failed to delete credential");
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.credentials.list.path] });
      toast({ title: "Deleted", description: "Credential removed" });
    },
  });
}
