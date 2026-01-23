import { Link } from "wouter";
import { Plus, Search, Layers, ArrowRight, Activity, Clock, Trash2 } from "lucide-react";
import { useWorkflows, useCreateWorkflow, useDeleteWorkflow } from "@/hooks/use-workflows";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";

export default function Home() {
  const { data: workflows, isLoading } = useWorkflows();
  const createMutation = useCreateWorkflow();
  const deleteMutation = useDeleteWorkflow();

  const handleCreate = async () => {
    try {
      const newWorkflow = await createMutation.mutateAsync({
        name: "Untitled Workflow",
        description: "New automation workflow",
        nodes: [],
        edges: []
      });
    } catch (e) {
      console.error(e);
    }
  };

  const handleDelete = async (e: React.MouseEvent, id: number) => {
    e.preventDefault();
    e.stopPropagation();
    if (confirm("Are you sure you want to delete this workflow?")) {
      await deleteMutation.mutateAsync(id);
    }
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Header */}
      <header className="border-b border-border bg-card/50 backdrop-blur sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
              <Layers className="w-5 h-5 text-white" />
            </div>
            <h1 className="text-xl font-bold tracking-tight">FlowAuto</h1>
          </div>
          
          <Button onClick={handleCreate} disabled={createMutation.isPending}>
            <Plus className="w-4 h-4 mr-2" />
            {createMutation.isPending ? "Creating..." : "New Workflow"}
          </Button>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-12">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h2 className="text-2xl font-bold">Your Workflows</h2>
            <p className="text-muted-foreground mt-1">Manage and monitor your automation pipelines</p>
          </div>
          <div className="relative w-64">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input placeholder="Search workflows..." className="pl-9 bg-card" />
          </div>
        </div>

        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[1, 2, 3].map(i => <Skeleton key={i} className="h-48 rounded-xl" />)}
          </div>
        ) : workflows?.length === 0 ? (
          <div className="text-center py-20 border border-dashed border-border rounded-3xl bg-card/30">
            <Layers className="w-16 h-16 mx-auto text-muted-foreground opacity-20 mb-4" />
            <h3 className="text-lg font-medium">No workflows yet</h3>
            <p className="text-muted-foreground mb-6">Create your first workflow to get started</p>
            <Button onClick={handleCreate} variant="outline">Create Workflow</Button>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {workflows?.map((workflow) => (
              <Link key={workflow.id} href={`/workflow/${workflow.id}`} className="group block">
                <article className="h-full bg-card hover:bg-muted/50 border border-border hover:border-primary/50 rounded-xl p-6 transition-all duration-300 hover:shadow-xl hover:shadow-primary/5 hover:-translate-y-1 relative overflow-hidden">
                  <div className="absolute top-0 right-0 p-4 flex gap-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8 text-muted-foreground hover:text-destructive opacity-0 group-hover:opacity-100 transition-all"
                      onClick={(e) => handleDelete(e, workflow.id)}
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                    <ArrowRight className="w-5 h-5 text-primary opacity-0 group-hover:opacity-100 transition-opacity" />
                  </div>
                  
                  <div className="flex items-center gap-3 mb-4 pr-8">
                    <div className="p-2 rounded-lg bg-primary/10 text-primary">
                      <Activity className="w-5 h-5" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-lg leading-tight group-hover:text-primary transition-colors">
                        {workflow.name}
                      </h3>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        ID: #{workflow.id}
                      </p>
                    </div>
                  </div>
                  
                  <p className="text-sm text-muted-foreground line-clamp-2 mb-6 h-10">
                    {workflow.description || "No description provided."}
                  </p>
                  
                  <div className="flex items-center justify-between pt-4 border-t border-border/50">
                    <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                      <Clock className="w-3.5 h-3.5" />
                      <span>Updated recently</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <div className="w-2 h-2 rounded-full bg-emerald-500" />
                      <span className="text-xs font-medium text-emerald-500">Active</span>
                    </div>
                  </div>
                </article>
              </Link>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
