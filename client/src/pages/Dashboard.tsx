import { AppLayout } from "@/components/layout/AppLayout";
import { useWorkflows, useExecutions } from "@/hooks/use-workflows";
import { Link } from "wouter";
import { 
  Plus, 
  Activity, 
  CheckCircle2, 
  XCircle, 
  Clock,
  ArrowRight
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatDistanceToNow } from "date-fns";
import { cn } from "@/lib/utils";

export default function Dashboard() {
  const { data: workflows, isLoading: loadingWorkflows } = useWorkflows();
  const { data: executions, isLoading: loadingExecutions } = useExecutions();

  const stats = [
    { 
      label: "Total Workflows", 
      value: workflows?.length || 0, 
      icon: Activity, 
      color: "text-blue-500", 
      bg: "bg-blue-500/10" 
    },
    { 
      label: "Executions (24h)", 
      value: executions?.filter(e => new Date(e.startedAt!).getTime() > Date.now() - 86400000).length || 0,
      icon: Clock, 
      color: "text-purple-500", 
      bg: "bg-purple-500/10" 
    },
    { 
      label: "Success Rate", 
      value: executions?.length 
        ? Math.round((executions.filter(e => e.status === 'completed').length / executions.length) * 100) + '%'
        : '0%',
      icon: CheckCircle2, 
      color: "text-green-500", 
      bg: "bg-green-500/10" 
    },
  ];

  return (
    <AppLayout>
      <div className="space-y-8">
        <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
          <div>
            <h1 className="text-3xl font-display font-bold text-foreground">Dashboard</h1>
            <p className="text-muted-foreground mt-1">Overview of your automation infrastructure.</p>
          </div>
          <Link href="/workflows/new">
            <Button size="lg" className="shadow-lg shadow-primary/20 hover:shadow-primary/30 transition-all">
              <Plus className="mr-2 h-4 w-4" /> Create Workflow
            </Button>
          </Link>
        </div>

        {/* Stats Grid */}
        <div className="grid gap-4 md:grid-cols-3">
          {stats.map((stat, i) => (
            <Card key={i} className="border-border/50 bg-card/50 backdrop-blur-sm shadow-sm hover:shadow-md transition-all">
              <CardContent className="p-6 flex items-center gap-4">
                <div className={cn("p-3 rounded-xl", stat.bg)}>
                  <stat.icon className={cn("h-6 w-6", stat.color)} />
                </div>
                <div>
                  <p className="text-sm font-medium text-muted-foreground">{stat.label}</p>
                  <h3 className="text-2xl font-bold font-display">{loadingWorkflows ? "-" : stat.value}</h3>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        <div className="grid gap-8 lg:grid-cols-2">
          {/* Recent Workflows */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold tracking-tight">Recent Workflows</h2>
              <Link href="/workflows" className="text-sm text-primary hover:underline flex items-center">
                View All <ArrowRight className="ml-1 h-3 w-3" />
              </Link>
            </div>
            
            {loadingWorkflows ? (
              Array(3).fill(0).map((_, i) => (
                <Skeleton key={i} className="h-20 w-full rounded-xl" />
              ))
            ) : workflows?.length === 0 ? (
              <div className="flex h-32 items-center justify-center rounded-xl border border-dashed text-muted-foreground">
                No workflows created yet.
              </div>
            ) : (
              <div className="space-y-3">
                {workflows?.slice(0, 5).map((workflow) => (
                  <Link key={workflow.id} href={`/workflows/${workflow.id}`}>
                    <div className="group flex items-center justify-between rounded-xl border border-border/50 bg-card p-4 hover:border-primary/50 hover:shadow-md transition-all cursor-pointer">
                      <div>
                        <h3 className="font-semibold text-foreground group-hover:text-primary transition-colors">
                          {workflow.name}
                        </h3>
                        <p className="text-sm text-muted-foreground line-clamp-1">
                          {workflow.description || "No description"}
                        </p>
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {workflow.createdAt && formatDistanceToNow(new Date(workflow.createdAt), { addSuffix: true })}
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </div>

          {/* Recent Executions */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold tracking-tight">Recent Executions</h2>
              <Link href="/executions" className="text-sm text-primary hover:underline flex items-center">
                View History <ArrowRight className="ml-1 h-3 w-3" />
              </Link>
            </div>

            {loadingExecutions ? (
              Array(3).fill(0).map((_, i) => (
                <Skeleton key={i} className="h-16 w-full rounded-xl" />
              ))
            ) : executions?.length === 0 ? (
              <div className="flex h-32 items-center justify-center rounded-xl border border-dashed text-muted-foreground">
                No executions yet.
              </div>
            ) : (
              <div className="space-y-3">
                {executions?.slice(0, 5).map((execution) => (
                  <Link key={execution.id} href={`/executions/${execution.id}`}>
                    <div className="flex items-center justify-between rounded-xl border border-border/50 bg-card p-4 hover:bg-accent/50 transition-colors cursor-pointer">
                      <div className="flex items-center gap-3">
                        <StatusIcon status={execution.status || 'pending'} />
                        <div>
                          <p className="font-medium text-sm">Execution #{execution.id}</p>
                          <p className="text-xs text-muted-foreground">
                            {execution.startedAt 
                              ? formatDistanceToNow(new Date(execution.startedAt), { addSuffix: true })
                              : 'Pending'
                            }
                          </p>
                        </div>
                      </div>
                      <div className="text-right text-xs">
                         <span className={cn(
                           "inline-flex items-center rounded-full px-2 py-1 font-medium",
                           getStatusColor(execution.status || 'pending')
                         )}>
                           {execution.status}
                         </span>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </AppLayout>
  );
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case 'completed': return <CheckCircle2 className="h-5 w-5 text-green-500" />;
    case 'failed': return <XCircle className="h-5 w-5 text-destructive" />;
    case 'running': return <Activity className="h-5 w-5 text-blue-500 animate-spin" />;
    default: return <Clock className="h-5 w-5 text-muted-foreground" />;
  }
}

function getStatusColor(status: string) {
  switch (status) {
    case 'completed': return "bg-green-500/10 text-green-600 dark:text-green-400";
    case 'failed': return "bg-destructive/10 text-destructive";
    case 'running': return "bg-blue-500/10 text-blue-600 dark:text-blue-400";
    default: return "bg-muted text-muted-foreground";
  }
}
