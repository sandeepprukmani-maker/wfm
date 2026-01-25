import { useWorkflows } from "@/hooks/use-workflows";
import { useExecutions } from "@/hooks/use-executions";
import { StatusBadge } from "@/components/StatusBadge";
import { Link } from "wouter";
import { 
  Plus, 
  ArrowRight, 
  Workflow, 
  Activity, 
  ServerCrash 
} from "lucide-react";
import { format } from "date-fns";

export default function Dashboard() {
  const { data: workflows, isLoading: loadingWorkflows } = useWorkflows();
  const { data: executions, isLoading: loadingExecutions } = useExecutions();

  const recentExecutions = executions?.slice(0, 5) || [];
  const stats = {
    totalWorkflows: workflows?.length || 0,
    activeExecutions: executions?.filter(e => e.status === 'running').length || 0,
    failedExecutions: executions?.filter(e => e.status === 'failed').length || 0,
  };

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">Dashboard</h2>
          <p className="text-muted-foreground mt-1">Overview of your orchestration system.</p>
        </div>
        <Link href="/workflows/new" className="inline-flex items-center gap-2 bg-primary text-primary-foreground px-4 py-2 rounded-xl font-medium shadow-lg hover:bg-primary/90 transition-all hover:scale-105 active:scale-95">
          <Plus className="w-5 h-5" />
          New Workflow
        </Link>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-card p-6 rounded-2xl border border-border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Total Workflows</p>
              <h3 className="text-3xl font-bold mt-2">{stats.totalWorkflows}</h3>
            </div>
            <div className="w-12 h-12 bg-primary/10 rounded-xl flex items-center justify-center">
              <Workflow className="w-6 h-6 text-primary" />
            </div>
          </div>
        </div>
        <div className="bg-card p-6 rounded-2xl border border-border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Active Runs</p>
              <h3 className="text-3xl font-bold mt-2">{stats.activeExecutions}</h3>
            </div>
            <div className="w-12 h-12 bg-blue-500/10 rounded-xl flex items-center justify-center">
              <Activity className="w-6 h-6 text-blue-500" />
            </div>
          </div>
        </div>
        <div className="bg-card p-6 rounded-2xl border border-border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Recent Failures</p>
              <h3 className="text-3xl font-bold mt-2">{stats.failedExecutions}</h3>
            </div>
            <div className="w-12 h-12 bg-red-500/10 rounded-xl flex items-center justify-center">
              <ServerCrash className="w-6 h-6 text-red-500" />
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Recent Workflows</h3>
            <Link href="/workflows" className="text-sm text-primary hover:underline flex items-center gap-1">
              View all <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
          <div className="space-y-3">
            {loadingWorkflows ? (
              <div className="text-center py-10 text-muted-foreground">Loading...</div>
            ) : workflows?.slice(0, 4).map(workflow => (
              <Link key={workflow.id} href={`/workflows/${workflow.id}`} className="block">
                <div className="bg-card hover:border-primary/50 transition-colors p-4 rounded-xl border border-border group">
                  <div className="flex justify-between items-center">
                    <span className="font-medium group-hover:text-primary transition-colors">{workflow.name}</span>
                    <span className="text-xs text-muted-foreground">
                      {format(new Date(workflow.createdAt!), 'MMM d, yyyy')}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground mt-1 truncate">
                    {workflow.description || "No description provided"}
                  </p>
                </div>
              </Link>
            ))}
          </div>
        </div>

        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Recent Executions</h3>
            <Link href="/executions" className="text-sm text-primary hover:underline flex items-center gap-1">
              View all <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
          <div className="space-y-3">
            {loadingExecutions ? (
              <div className="text-center py-10 text-muted-foreground">Loading...</div>
            ) : recentExecutions.map(exec => (
              <div key={exec.id} className="bg-card p-4 rounded-xl border border-border flex items-center justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <span className="font-medium text-sm">Execution #{exec.id}</span>
                  </div>
                  <span className="text-xs text-muted-foreground block mt-1">
                    {format(new Date(exec.startedAt!), 'MMM d, HH:mm:ss')}
                  </span>
                </div>
                <StatusBadge status={exec.status} />
              </div>
            ))}
            {recentExecutions.length === 0 && (
              <div className="text-center py-8 text-muted-foreground bg-muted/20 rounded-xl border-dashed border border-border">
                No executions yet
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
