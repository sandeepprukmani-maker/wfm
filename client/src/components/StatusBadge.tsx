import { cn } from "@/lib/utils";
import { CheckCircle2, XCircle, Clock, Loader2 } from "lucide-react";

type Status = "pending" | "running" | "completed" | "failed";

export function StatusBadge({ status }: { status: string }) {
  const normalizedStatus = status.toLowerCase() as Status;
  
  const config = {
    pending: {
      color: "text-yellow-500 bg-yellow-500/10 border-yellow-500/20",
      icon: Clock,
      label: "Pending"
    },
    running: {
      color: "text-blue-500 bg-blue-500/10 border-blue-500/20",
      icon: Loader2,
      label: "Running",
      animate: true
    },
    completed: {
      color: "text-green-500 bg-green-500/10 border-green-500/20",
      icon: CheckCircle2,
      label: "Completed"
    },
    failed: {
      color: "text-red-500 bg-red-500/10 border-red-500/20",
      icon: XCircle,
      label: "Failed"
    }
  };

  const current = config[normalizedStatus] || config.pending;
  const Icon = current.icon;

  return (
    <div className={cn(
      "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium border",
      current.color
    )}>
      <Icon className={cn("w-3.5 h-3.5", current.animate && "animate-spin")} />
      {current.label}
    </div>
  );
}
