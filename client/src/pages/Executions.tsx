import { useExecutions } from "@/hooks/use-executions";
import { StatusBadge } from "@/components/StatusBadge";
import { format } from "date-fns";
import { 
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { ExternalLink, Download } from "lucide-react";

export default function Executions() {
  const { data: executions, isLoading } = useExecutions();

  const handleExport = (id: number) => {
    window.location.href = `/api/executions/${id}/export`;
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold tracking-tight">Execution History</h2>
        <p className="text-muted-foreground mt-1">Audit logs of all workflow runs.</p>
      </div>

      <div className="bg-card border border-border rounded-xl overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Execution ID</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Started At</TableHead>
              <TableHead>Duration</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center py-8">Loading...</TableCell>
              </TableRow>
            ) : executions?.map((exec) => (
              <TableRow key={exec.id}>
                <TableCell className="font-mono text-xs">#{exec.id}</TableCell>
                <TableCell><StatusBadge status={exec.status} /></TableCell>
                <TableCell className="text-muted-foreground text-sm">
                  {exec.startedAt ? format(new Date(exec.startedAt), 'PP pp') : '-'}
                </TableCell>
                <TableCell className="text-muted-foreground text-sm">
                  {exec.completedAt && exec.startedAt
                    ? `${((new Date(exec.completedAt).getTime() - new Date(exec.startedAt).getTime()) / 1000).toFixed(1)}s` 
                    : '-'
                  }
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex justify-end gap-2">
                    <Button 
                      size="sm" 
                      variant="outline" 
                      className="h-8 gap-2"
                      onClick={() => handleExport(exec.id)}
                    >
                      <Download className="w-4 h-4" />
                      Export
                    </Button>
                    <Link href={`/workflows/${exec.workflowId}?executionId=${exec.id}`}>
                      <Button size="sm" variant="ghost" className="h-8 w-8 p-0">
                        <ExternalLink className="w-4 h-4" />
                      </Button>
                    </Link>
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {executions?.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} className="text-center py-8 text-muted-foreground">
                  No execution history found.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
