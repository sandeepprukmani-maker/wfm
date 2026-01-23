import { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Zap, Globe, GitBranch, Cpu, Database, Cloud, Wind } from 'lucide-react';
import { cn } from '@/lib/utils';

const NodeHeader = ({ icon: Icon, title, colorClass }: { icon: any, title: string, colorClass: string }) => (
  <div className={cn("flex items-center gap-2 px-4 py-2 border-b border-border rounded-t-xl bg-opacity-10", colorClass)}>
    <Icon className="w-4 h-4" />
    <span className="text-xs font-bold uppercase tracking-wider">{title}</span>
  </div>
);

const NodeContent = ({ children }: { children: React.ReactNode }) => (
  <div className="p-4 bg-card rounded-b-xl min-w-[200px]">
    {children}
  </div>
);

export const TriggerNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="source" position={Position.Bottom} className="!bg-emerald-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Zap} title="Trigger" colorClass="bg-emerald-500/10 text-emerald-500" />
        <NodeContent>
          <div className="text-sm font-medium">{data.label || "Manual Trigger"}</div>
          <div className="text-xs text-muted-foreground mt-1">Starts the workflow</div>
        </NodeContent>
      </div>
    </div>
  );
});

export const HttpNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-blue-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Globe} title="HTTP Request" colorClass="bg-blue-500/10 text-blue-500" />
        <NodeContent>
          <div className="flex items-center gap-2 mb-1">
            <span className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-500">
              {data.method || "GET"}
            </span>
            <span className="text-xs font-mono text-muted-foreground truncate max-w-[140px]" title={data.url}>
              {data.url || "/api/..."}
            </span>
          </div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-blue-500" />
    </div>
  );
});

export const LogicNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-orange-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={GitBranch} title="Logic" colorClass="bg-orange-500/10 text-orange-500" />
        <NodeContent>
          <div className="text-sm font-medium">If {data.condition || "condition"}</div>
          <div className="text-xs text-muted-foreground mt-1">Then continue</div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} id="true" style={{ left: '30%' }} className="!bg-emerald-500" />
      <Handle type="source" position={Position.Bottom} id="false" style={{ left: '70%' }} className="!bg-rose-500" />
    </div>
  );
});

export const TransformNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-purple-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Cpu} title="Transform" colorClass="bg-purple-500/10 text-purple-500" />
        <NodeContent>
          <div className="text-sm font-medium">Process Data</div>
          <div className="text-xs font-mono text-muted-foreground mt-1 max-w-[150px] truncate">
            {data.code ? "Custom JS" : "No code"}
          </div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-purple-500" />
    </div>
  );
});

export const S3Node = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-amber-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Cloud} title="AWS S3" colorClass="bg-amber-500/10 text-amber-500" />
        <NodeContent>
          <div className="text-sm font-medium">{data.operation || "List Buckets"}</div>
          <div className="text-xs text-muted-foreground mt-1 truncate max-w-[150px]">
            {data.bucket || "No bucket selected"}
          </div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-amber-500" />
    </div>
  );
});

export const DatabaseNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-indigo-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Database} title="Database" colorClass="bg-indigo-500/10 text-indigo-500" />
        <NodeContent>
          <div className="text-sm font-medium">{data.dbType || "SQL"}</div>
          <div className="text-xs font-mono text-muted-foreground mt-1 truncate max-w-[150px]">
            {data.query || "No query"}
          </div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-indigo-500" />
    </div>
  );
});

export const AirflowNode = memo(({ data, selected }: NodeProps) => {
  return (
    <div className={cn("rounded-xl border transition-all duration-200", selected ? "border-primary ring-2 ring-primary/20" : "border-border shadow-md")}>
      <Handle type="target" position={Position.Top} className="!bg-sky-500" />
      <div className="bg-card rounded-xl overflow-hidden">
        <NodeHeader icon={Wind} title="Airflow" colorClass="bg-sky-500/10 text-sky-500" />
        <NodeContent>
          <div className="text-sm font-medium">{data.operation || "Trigger DAG"}</div>
          <div className="text-xs text-muted-foreground mt-1 truncate max-w-[150px]">
            {data.dagId || "No DAG ID"}
          </div>
        </NodeContent>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-sky-500" />
    </div>
  );
});
