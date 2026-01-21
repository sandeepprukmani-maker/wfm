import { memo } from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import { 
  Database, 
  Code2, 
  CheckCircle2, 
  PlayCircle,
  AlertTriangle,
  Server
} from 'lucide-react';
import { cn } from '@/lib/utils';

// Map node types to icons and colors
const NODE_CONFIG = {
  sql: { icon: Database, color: 'text-blue-500', bg: 'bg-blue-500/10', border: 'border-blue-200 dark:border-blue-900' },
  script: { icon: Code2, color: 'text-yellow-500', bg: 'bg-yellow-500/10', border: 'border-yellow-200 dark:border-yellow-900' },
  assertion: { icon: CheckCircle2, color: 'text-green-500', bg: 'bg-green-500/10', border: 'border-green-200 dark:border-green-900' },
  api: { icon: Server, color: 'text-purple-500', bg: 'bg-purple-500/10', border: 'border-purple-200 dark:border-purple-900' },
  trigger: { icon: PlayCircle, color: 'text-primary', bg: 'bg-primary/10', border: 'border-primary/20' },
};

const CustomNode = ({ data, selected }: NodeProps) => {
  const type = data.type as keyof typeof NODE_CONFIG || 'script';
  const config = NODE_CONFIG[type];
  const Icon = config.icon;

  return (
    <div className={cn(
      "w-64 rounded-xl border-2 bg-card p-4 transition-all duration-200",
      config.border,
      selected ? "shadow-xl ring-2 ring-primary ring-offset-2 ring-offset-background" : "shadow-sm hover:shadow-md"
    )}>
      <Handle type="target" position={Position.Top} className="!bg-muted-foreground !w-3 !h-3 !-top-2.5" />
      
      <div className="flex items-start gap-3">
        <div className={cn("rounded-lg p-2.5", config.bg)}>
          <Icon className={cn("h-5 w-5", config.color)} />
        </div>
        <div className="flex-1 overflow-hidden">
          <h3 className="font-semibold text-sm truncate pr-2">{data.label as string}</h3>
          <p className="text-xs text-muted-foreground truncate mt-0.5">
            {data.description as string || type.toUpperCase()}
          </p>
        </div>
        {data.error && (
          <AlertTriangle className="h-4 w-4 text-destructive shrink-0 animate-pulse" />
        )}
      </div>

      <Handle type="source" position={Position.Bottom} className="!bg-muted-foreground !w-3 !h-3 !-bottom-2.5" />
    </div>
  );
};

export default memo(CustomNode);
