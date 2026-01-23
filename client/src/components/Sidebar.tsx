import { Zap, Globe, GitBranch, Cpu, Database, Cloud, Wind } from 'lucide-react';

const DraggableNode = ({ type, label, icon: Icon, colorClass }: { type: string, label: string, icon: any, colorClass: string }) => {
  const onDragStart = (event: React.DragEvent, nodeType: string) => {
    event.dataTransfer.setData('application/reactflow', nodeType);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div 
      className="flex items-center gap-3 p-3 mb-3 bg-muted/30 hover:bg-muted border border-border/50 hover:border-border rounded-lg cursor-grab active:cursor-grabbing transition-all duration-200 group"
      onDragStart={(event) => onDragStart(event, type)}
      draggable
    >
      <div className={`p-2 rounded-md ${colorClass} bg-opacity-10 group-hover:bg-opacity-20 transition-colors`}>
        <Icon className={`w-4 h-4 ${colorClass.replace('bg-', 'text-')}`} />
      </div>
      <div>
        <div className="text-sm font-medium text-foreground">{label}</div>
        <div className="text-[10px] text-muted-foreground">Drag to add</div>
      </div>
    </div>
  );
};

export default function Sidebar() {
  return (
    <aside className="w-64 border-r border-border bg-card/50 backdrop-blur-sm p-4 flex flex-col gap-6 overflow-y-auto">
      <div>
        <h2 className="text-xs font-bold text-muted-foreground uppercase tracking-widest mb-4">Core Nodes</h2>
        <DraggableNode type="trigger" label="Trigger" icon={Zap} colorClass="bg-emerald-500" />
        <DraggableNode type="http" label="HTTP Request" icon={Globe} colorClass="bg-blue-500" />
        <DraggableNode type="logic" label="Logic / Branch" icon={GitBranch} colorClass="bg-orange-500" />
        <DraggableNode type="transform" label="Transform Data" icon={Cpu} colorClass="bg-purple-500" />
      </div>

      <div>
        <h2 className="text-xs font-bold text-muted-foreground uppercase tracking-widest mb-4">Integrations</h2>
        <DraggableNode type="s3" label="AWS S3" icon={Cloud} colorClass="bg-amber-500" />
        <DraggableNode type="db" label="Database (SQL)" icon={Database} colorClass="bg-indigo-500" />
        <DraggableNode type="airflow" label="Airflow" icon={Wind} colorClass="bg-sky-500" />
      </div>
      
      <div className="mt-auto pt-6">
        <div className="p-4 rounded-xl bg-gradient-to-br from-primary/10 to-transparent border border-primary/20">
          <h3 className="text-sm font-semibold text-primary mb-1">API Testing</h3>
          <p className="text-xs text-muted-foreground">
            Enter a Swagger URL in settings and use AI prompts to generate test cases.
          </p>
        </div>
      </div>
    </aside>
  );
}
