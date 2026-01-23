import { format } from "date-fns";
import { useWorkflowExecutions } from "@/hooks/use-workflows";
import { CheckCircle2, XCircle, Clock, ChevronRight, ChevronDown } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useState } from "react";

interface ExecutionsListProps {
  workflowId: number;
}

export default function ExecutionsList({ workflowId }: ExecutionsListProps) {
  const { data: executions, isLoading } = useWorkflowExecutions(workflowId);
  const [expandedExec, setExpandedExec] = useState<number | null>(null);

  if (isLoading) {
    return (
      <div className="p-4 space-y-3">
        {[1, 2, 3].map(i => <Skeleton key={i} className="h-12 w-full rounded-lg" />)}
      </div>
    );
  }

  if (!executions?.length) {
    return (
      <div className="h-full flex flex-col items-center justify-center text-muted-foreground p-8">
        <Clock className="w-8 h-8 mb-2 opacity-50" />
        <p className="text-sm">No executions yet</p>
        <p className="text-xs opacity-70">Run the workflow to see logs</p>
      </div>
    );
  }

  return (
    <ScrollArea className="h-full">
      <div className="flex flex-col p-2 gap-2">
        {executions.map((exec) => (
          <div 
            key={exec.id} 
            className="border border-border/50 rounded-lg overflow-hidden bg-card/30"
          >
            <div 
              className="flex items-center gap-3 p-3 hover:bg-muted/50 cursor-pointer transition-colors group"
              onClick={() => setExpandedExec(expandedExec === exec.id ? null : exec.id)}
            >
              <div className="flex-shrink-0">
                {exec.status === 'completed' && <CheckCircle2 className="w-5 h-5 text-emerald-500" />}
                {exec.status === 'failed' && <XCircle className="w-5 h-5 text-rose-500" />}
                {exec.status === 'running' && <div className="w-5 h-5 rounded-full border-2 border-primary border-t-transparent animate-spin" />}
                {exec.status === 'pending' && <div className="w-5 h-5 rounded-full border-2 border-muted" />}
              </div>
              
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between">
                  <span className={cn(
                    "text-sm font-medium capitalize",
                    exec.status === 'completed' ? "text-emerald-500" :
                    exec.status === 'failed' ? "text-rose-500" : "text-foreground"
                  )}>
                    {exec.status}
                  </span>
                  <span className="text-[10px] text-muted-foreground font-mono">
                    {exec.startedAt ? format(new Date(exec.startedAt), 'HH:mm:ss') : '-'}
                  </span>
                </div>
                <div className="text-xs text-muted-foreground truncate mt-0.5">
                  ID: {exec.id} • {exec.logs ? `${(exec.logs as any[]).length} steps` : 'No logs'}
                </div>
              </div>
              
              {expandedExec === exec.id ? (
                <ChevronDown className="w-4 h-4 text-muted-foreground transition-all" />
              ) : (
                <ChevronRight className="w-4 h-4 text-muted-foreground transition-all" />
              )}
            </div>

            {expandedExec === exec.id && exec.logs && (
              <div className="border-t border-border/50 bg-muted/20 p-3 space-y-3 animate-in slide-in-from-top-1 duration-200">
                {(exec.logs as any[]).map((log, idx) => (
                  <div key={idx} className="space-y-1">
                    <div className="flex items-center gap-2">
                      <div className={cn(
                        "w-1.5 h-1.5 rounded-full",
                        log.type === 'success' ? "bg-emerald-500" :
                        log.type === 'error' ? "bg-rose-500" :
                        log.type === 'warning' ? "bg-amber-500" : "bg-blue-500"
                      )} />
                      <span className="text-[10px] font-mono text-muted-foreground">
                        {format(new Date(log.timestamp), 'HH:mm:ss.SSS')}
                      </span>
                      <span className="text-xs font-medium text-foreground">{log.message}</span>
                    </div>
                    {log.data && (
                      <div className="ml-3.5 mt-1 p-2 rounded bg-background/50 border border-border/50 overflow-x-auto">
                        <pre className="text-[10px] font-mono text-muted-foreground leading-relaxed">
                          {JSON.stringify(log.data, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </ScrollArea>
  );
}
