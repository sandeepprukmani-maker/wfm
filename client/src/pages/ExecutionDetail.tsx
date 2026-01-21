import { AppLayout } from "@/components/layout/AppLayout";
import { useExecution } from "@/hooks/use-workflows";
import { useRoute, Link } from "wouter";
import { ArrowLeft, Terminal, AlertTriangle, CheckCircle2, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { ExecutionLog } from "@shared/schema";

export default function ExecutionDetail() {
  const [match, params] = useRoute("/executions/:id");
  const id = params?.id ? parseInt(params.id) : 0;
  const { data: execution, isLoading } = useExecution(id);

  if (isLoading) {
    return (
      <AppLayout>
        <div className="space-y-6">
          <Skeleton className="h-10 w-48" />
          <div className="grid gap-6 md:grid-cols-2">
            <Skeleton className="h-64 w-full" />
            <Skeleton className="h-64 w-full" />
          </div>
        </div>
      </AppLayout>
    );
  }

  if (!execution) return <div>Execution not found</div>;

  const logs = (execution.logs as ExecutionLog[]) || [];
  const stepStatus = (execution.stepStatus as any) || {};

  return (
    <AppLayout>
      <div className="space-y-6">
        <div className="flex items-center gap-4">
          <Link href="/executions">
            <Button variant="ghost" size="icon">
              <ArrowLeft className="h-5 w-5" />
            </Button>
          </Link>
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-display font-bold">Execution #{id}</h1>
              <Badge 
                className={cn(
                  "capitalize text-base px-3 py-0.5",
                  execution.status === 'completed' && "bg-green-500",
                  execution.status === 'failed' && "bg-destructive",
                  execution.status === 'running' && "bg-blue-500 animate-pulse"
                )}
              >
                {execution.status}
              </Badge>
            </div>
            <p className="text-muted-foreground mt-1">
              Started {execution.startedAt ? new Date(execution.startedAt).toLocaleString() : '-'}
            </p>
          </div>
        </div>

        <div className="grid gap-6 lg:grid-cols-3">
          {/* Logs Panel */}
          <Card className="lg:col-span-2 border-border/50 bg-card shadow-sm flex flex-col h-[600px]">
            <CardHeader className="border-b bg-muted/20 py-3">
              <div className="flex items-center gap-2">
                <Terminal className="h-4 w-4 text-muted-foreground" />
                <CardTitle className="text-base">Console Logs</CardTitle>
              </div>
            </CardHeader>
            <CardContent className="p-0 flex-1 overflow-hidden">
              <ScrollArea className="h-full bg-[#0d1117] text-gray-300 font-mono text-sm p-4">
                {logs.length === 0 ? (
                  <div className="text-gray-600 italic">No logs available...</div>
                ) : (
                  logs.map((log, i) => (
                    <div key={i} className="mb-2 flex items-start gap-3 border-l-2 border-transparent pl-2 hover:bg-white/5 py-1 rounded-r">
                      <span className="text-gray-500 shrink-0 text-xs mt-0.5">
                        {new Date(log.timestamp).toLocaleTimeString()}
                      </span>
                      <span className={cn(
                        "break-all",
                        log.level === 'error' && "text-red-400",
                        log.level === 'warn' && "text-yellow-400",
                        log.level === 'info' && "text-blue-300"
                      )}>
                        [{log.level.toUpperCase()}] {log.stepId ? `(${log.stepId}) ` : ''}{log.message}
                      </span>
                    </div>
                  ))
                )}
              </ScrollArea>
            </CardContent>
          </Card>

          {/* Steps Status */}
          <div className="space-y-6">
            <Card className="border-border/50 bg-card shadow-sm">
              <CardHeader className="border-b bg-muted/20 py-3">
                <CardTitle className="text-base">Step Breakdown</CardTitle>
              </CardHeader>
              <CardContent className="p-4 space-y-4">
                {Object.entries(stepStatus).length === 0 ? (
                  <div className="text-muted-foreground text-sm">No steps recorded yet.</div>
                ) : (
                  Object.entries(stepStatus).map(([stepId, status]: [string, any]) => (
                    <div key={stepId} className="flex items-center justify-between p-3 rounded-lg border bg-background/50">
                      <div className="flex items-center gap-3">
                        {status.status === 'completed' && <CheckCircle2 className="h-4 w-4 text-green-500" />}
                        {status.status === 'failed' && <AlertTriangle className="h-4 w-4 text-destructive" />}
                        {status.status === 'running' && <Clock className="h-4 w-4 text-blue-500 animate-spin" />}
                        {status.status === 'pending' && <div className="h-4 w-4 rounded-full border-2 border-muted" />}
                        
                        <div>
                          <p className="text-sm font-medium">{stepId}</p>
                          <p className="text-xs text-muted-foreground capitalize">{status.status}</p>
                        </div>
                      </div>
                      <div className="text-xs font-mono text-muted-foreground">
                        {status.startTime ? new Date(status.startTime).toLocaleTimeString().split(' ')[0] : '-'}
                      </div>
                    </div>
                  ))
                )}
              </CardContent>
            </Card>

            <Card className="border-border/50 bg-card shadow-sm">
              <CardHeader className="border-b bg-muted/20 py-3">
                 <CardTitle className="text-base">Output</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <div className="bg-muted/30 p-4 font-mono text-xs overflow-auto max-h-60">
                  <pre>{JSON.stringify(execution.outputs || {}, null, 2)}</pre>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </AppLayout>
  );
}
