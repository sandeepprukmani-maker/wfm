import { useWorkflows } from "@/hooks/use-workflows";
import { useExecutions } from "@/hooks/use-executions";
import { StatusBadge } from "@/components/StatusBadge";
import { Link } from "wouter";
import { 
  Activity, 
  CheckCircle2, 
  XCircle, 
  Clock, 
  Workflow,
  ArrowRight,
  ChevronRight
} from "lucide-react";
import { format } from "date-fns";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export default function ReplitDashboard() {
  const { data: workflows, isLoading: loadingWorkflows } = useWorkflows();
  const { data: executions, isLoading: loadingExecutions } = useExecutions();

  const totalExecutions = executions?.length || 0;
  const passedExecutions = executions?.filter(e => e.status === 'completed').length || 0;
  const failedExecutions = executions?.filter(e => e.status === 'failed').length || 0;
  const runningExecutions = executions?.filter(e => e.status === 'running').length || 0;

  const successRate = totalExecutions > 0 
    ? Math.round((passedExecutions / totalExecutions) * 100) 
    : 0;

  const recentExecutions = executions?.slice(0, 10) || [];

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1">
        <h2 className="text-2xl font-bold tracking-tight">Overview</h2>
        <p className="text-sm text-muted-foreground">Track your workflow performance and history.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="hover-elevate">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-green-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{successRate}%</div>
            <p className="text-xs text-muted-foreground mt-1">
              Overall execution success
            </p>
          </CardContent>
        </Card>
        <Card className="hover-elevate">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Failed</CardTitle>
            <XCircle className="h-4 w-4 text-destructive" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{failedExecutions}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Total failed attempts
            </p>
          </CardContent>
        </Card>
        <Card className="hover-elevate">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Running</CardTitle>
            <Clock className="h-4 w-4 text-blue-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{runningExecutions}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Currently active runs
            </p>
          </CardContent>
        </Card>
        <Card className="hover-elevate">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Workflows</CardTitle>
            <Workflow className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{workflows?.length || 0}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Managed pipelines
            </p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
              <CardTitle className="text-base font-semibold">Run History</CardTitle>
              <Link href="/executions">
                <Button variant="ghost" size="sm" className="gap-1 h-8">
                  View full history <ArrowRight className="w-3 h-3" />
                </Button>
              </Link>
            </CardHeader>
            <CardContent className="p-0">
              <div className="divide-y border-t">
                {loadingExecutions ? (
                  <div className="p-8 text-center text-muted-foreground">Loading history...</div>
                ) : recentExecutions.map((exec) => (
                  <Link key={exec.id} href={`/executions`}>
                    <div className="flex items-center gap-4 p-4 hover:bg-muted/50 transition-colors cursor-pointer group">
                      <div className={cn(
                        "w-2 h-2 rounded-full shrink-0",
                        exec.status === 'completed' ? "bg-green-500" : 
                        exec.status === 'failed' ? "bg-destructive" : "bg-blue-500"
                      )} />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium truncate">Execution #{exec.id}</p>
                        <p className="text-xs text-muted-foreground">
                          {format(new Date(exec.startedAt!), 'MMM d, HH:mm:ss')}
                        </p>
                      </div>
                      <StatusBadge status={exec.status} />
                      <ChevronRight className="w-4 h-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                    </div>
                  </Link>
                ))}
                {recentExecutions.length === 0 && (
                  <div className="p-8 text-center text-muted-foreground">No recent runs</div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base font-semibold">Active Workflows</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <div className="divide-y border-t">
                {loadingWorkflows ? (
                  <div className="p-8 text-center text-muted-foreground">Loading workflows...</div>
                ) : workflows?.slice(0, 5).map((wf) => (
                  <Link key={wf.id} href={`/workflows/${wf.id}`}>
                    <div className="flex items-center gap-3 p-4 hover:bg-muted/50 transition-colors cursor-pointer group">
                      <div className="w-8 h-8 rounded-lg bg-muted flex items-center justify-center shrink-0">
                        <Workflow className="w-4 h-4 text-muted-foreground group-hover:text-primary transition-colors" />
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium truncate">{wf.name}</p>
                        <p className="text-xs text-muted-foreground truncate">
                          Updated {format(new Date(wf.updatedAt!), 'MMM d')}
                        </p>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
