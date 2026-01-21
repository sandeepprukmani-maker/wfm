import { AppLayout } from "@/components/layout/AppLayout";
import { useExecutions } from "@/hooks/use-workflows";
import { Link } from "wouter";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { format } from "date-fns";
import { ArrowRight, Terminal } from "lucide-react";
import { cn } from "@/lib/utils";

export default function ExecutionsList() {
  const { data: executions, isLoading } = useExecutions();

  return (
    <AppLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-display font-bold">Execution History</h1>
          <p className="text-muted-foreground mt-1">Monitor past and running workflow executions.</p>
        </div>

        <div className="rounded-xl border bg-card shadow-sm overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="bg-muted/50 hover:bg-muted/50">
                <TableHead>Execution ID</TableHead>
                <TableHead>Workflow ID</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Started At</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array(5).fill(0).map((_, i) => (
                  <TableRow key={i}>
                    <TableCell><Skeleton className="h-4 w-12" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-8" /></TableCell>
                    <TableCell><Skeleton className="h-6 w-20 rounded-full" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                    <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                    <TableCell><Skeleton className="h-8 w-8 ml-auto" /></TableCell>
                  </TableRow>
                ))
              ) : executions?.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-32 text-center text-muted-foreground">
                    No executions found.
                  </TableCell>
                </TableRow>
              ) : (
                executions?.map((exec) => {
                  const duration = exec.completedAt && exec.startedAt
                    ? ((new Date(exec.completedAt).getTime() - new Date(exec.startedAt).getTime()) / 1000).toFixed(2) + 's'
                    : '-';
                  
                  return (
                    <TableRow key={exec.id} className="hover:bg-muted/30">
                      <TableCell className="font-mono text-xs">#{exec.id}</TableCell>
                      <TableCell>
                        <Link href={`/workflows/${exec.workflowId}`} className="hover:underline">
                          WF-{exec.workflowId}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <Badge 
                          variant="outline" 
                          className={cn(
                            "capitalize",
                            exec.status === 'completed' && "border-green-500 text-green-600 bg-green-500/10",
                            exec.status === 'failed' && "border-destructive text-destructive bg-destructive/10",
                            exec.status === 'running' && "border-blue-500 text-blue-600 bg-blue-500/10 animate-pulse"
                          )}
                        >
                          {exec.status}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm">
                        {exec.startedAt ? format(new Date(exec.startedAt), 'MMM d, HH:mm:ss') : '-'}
                      </TableCell>
                      <TableCell className="text-muted-foreground text-sm font-mono">{duration}</TableCell>
                      <TableCell className="text-right">
                        <Link href={`/executions/${exec.id}`}>
                          <Button size="icon" variant="ghost" className="h-8 w-8">
                            <ArrowRight className="h-4 w-4" />
                          </Button>
                        </Link>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </AppLayout>
  );
}
