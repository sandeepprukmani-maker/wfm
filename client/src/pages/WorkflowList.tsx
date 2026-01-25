import { useWorkflows, useDeleteWorkflow, useCreateWorkflow } from "@/hooks/use-workflows";
import { Link, useLocation } from "wouter";
import { 
  Plus, 
  MoreVertical, 
  Pencil, 
  Trash2, 
  Play,
  Search,
  ArrowRight,
  Workflow
} from "lucide-react";
import { format } from "date-fns";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";

const createSchema = z.object({
  name: z.string().min(1, "Name is required"),
  description: z.string().optional(),
});

export default function WorkflowList() {
  const { data: workflows, isLoading } = useWorkflows();
  const { mutateAsync: deleteWorkflow } = useDeleteWorkflow();
  const { mutateAsync: createWorkflow } = useCreateWorkflow();
  const [, setLocation] = useLocation();
  const [search, setSearch] = useState("");
  const [isCreateOpen, setIsCreateOpen] = useState(false);

  const form = useForm<z.infer<typeof createSchema>>({
    resolver: zodResolver(createSchema),
    defaultValues: { name: "", description: "" }
  });

  const handleCreate = async (data: z.infer<typeof createSchema>) => {
    try {
      const newWorkflow = await createWorkflow({
        ...data,
        nodes: [],
        edges: []
      });
      setIsCreateOpen(false);
      setLocation(`/workflows/${newWorkflow.id}`);
    } catch (e) {
      // handled by hook
    }
  };

  const filtered = workflows?.filter(w => 
    w.name.toLowerCase().includes(search.toLowerCase()) || 
    w.description?.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="space-y-8">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">Workflows</h2>
          <p className="text-muted-foreground mt-1">Manage and execute your automation pipelines.</p>
        </div>
        
        <Dialog open={isCreateOpen} onOpenChange={setIsCreateOpen}>
          <DialogTrigger asChild>
            <Button className="bg-primary text-primary-foreground shadow-lg hover:shadow-primary/25">
              <Plus className="w-4 h-4 mr-2" />
              New Workflow
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create Workflow</DialogTitle>
            </DialogHeader>
            <form onSubmit={form.handleSubmit(handleCreate)} className="space-y-4 mt-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Name</label>
                <Input {...form.register("name")} placeholder="My Awesome Pipeline" />
                {form.formState.errors.name && (
                  <p className="text-xs text-destructive">{form.formState.errors.name.message}</p>
                )}
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Description</label>
                <Input {...form.register("description")} placeholder="What does this workflow do?" />
              </div>
              <div className="flex justify-end gap-2 pt-2">
                <Button variant="outline" type="button" onClick={() => setIsCreateOpen(false)}>Cancel</Button>
                <Button type="submit" disabled={form.formState.isSubmitting}>
                  {form.formState.isSubmitting ? "Creating..." : "Create"}
                </Button>
              </div>
            </form>
          </DialogContent>
        </Dialog>
      </div>

      <div className="flex items-center gap-4 bg-card p-2 rounded-xl border border-border">
        <Search className="w-5 h-5 text-muted-foreground ml-2" />
        <input 
          className="bg-transparent border-none focus:outline-none flex-1 text-sm h-9"
          placeholder="Search workflows..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[1,2,3].map(i => (
            <div key={i} className="h-40 bg-muted/20 animate-pulse rounded-2xl" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filtered?.map((workflow) => (
            <div key={workflow.id} className="group bg-card border border-border rounded-2xl p-6 hover:shadow-xl transition-all duration-300 flex flex-col relative overflow-hidden">
              <div className="absolute top-0 left-0 w-1 h-full bg-primary opacity-0 group-hover:opacity-100 transition-opacity" />
              
              <div className="flex justify-between items-start mb-4">
                <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-gray-800 to-black border border-border flex items-center justify-center">
                  <Workflow className="w-5 h-5 text-muted-foreground group-hover:text-primary transition-colors" />
                </div>
                <DropdownMenu>
                  <DropdownMenuTrigger className="p-1 hover:bg-muted rounded-md opacity-0 group-hover:opacity-100 transition-opacity">
                    <MoreVertical className="w-4 h-4 text-muted-foreground" />
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onClick={() => setLocation(`/workflows/${workflow.id}`)}>
                      <Pencil className="w-4 h-4 mr-2" /> Edit
                    </DropdownMenuItem>
                    <DropdownMenuItem 
                      className="text-destructive focus:text-destructive"
                      onClick={() => {
                        if(confirm('Are you sure?')) deleteWorkflow(workflow.id);
                      }}
                    >
                      <Trash2 className="w-4 h-4 mr-2" /> Delete
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>

              <h3 className="font-bold text-lg mb-1">{workflow.name}</h3>
              <p className="text-sm text-muted-foreground line-clamp-2 mb-6 flex-1">
                {workflow.description || "No description"}
              </p>

              <div className="flex items-center justify-between pt-4 border-t border-border/50">
                <span className="text-xs text-muted-foreground">
                  Updated {format(new Date(workflow.updatedAt!), 'MMM d')}
                </span>
                <Link href={`/workflows/${workflow.id}`}>
                   <Button size="sm" variant="ghost" className="h-8 gap-1 hover:bg-primary/10 hover:text-primary">
                     Open <ArrowRight className="w-3 h-3" />
                   </Button>
                </Link>
              </div>
            </div>
          ))}
          {filtered?.length === 0 && (
            <div className="col-span-full py-12 text-center text-muted-foreground">
              No workflows found matching "{search}"
            </div>
          )}
        </div>
      )}
    </div>
  );
}
