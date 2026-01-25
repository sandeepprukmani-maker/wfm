import { useCallback, useEffect, useState } from 'react';
import ReactFlow, { 
  Background, 
  Controls, 
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  Edge,
  Panel
} from 'reactflow';
import 'reactflow/dist/style.css';
import { useWorkflow, useUpdateWorkflow, useGenerateWorkflow, useExecuteWorkflow } from '@/hooks/use-workflows';
import { useExecution } from '@/hooks/use-executions';
import { useRoute } from 'wouter';
import { 
  Play, 
  Save, 
  Sparkles, 
  Loader2,
  Terminal,
  Database,
  Cloud,
  ArrowRight as ArrowRightIcon,
  ChevronRight,
  GitBranch,
  Search as SearchIcon,
  Download
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { Handle, Position } from 'reactflow';

const NodeIcon = ({ type, className }: { type: string, className?: string }) => {
  switch(type) {
    case 'airflow_trigger': return <Cloud className={cn("w-5 h-5", className)} />;
    case 'sql_query': return <Database className={cn("w-5 h-5", className)} />;
    case 'python_script': return <Terminal className={cn("w-5 h-5", className)} />;
    case 'condition': return <GitBranch className={cn("w-5 h-5", className)} />;
    default: return <Sparkles className={cn("w-5 h-5", className)} />;
  }
};

const CustomNode = ({ data, type, selected }: any) => {
  const isAirflow = type === 'airflow_trigger';
  const isSQL = type === 'sql_query';
  const isPython = type === 'python_script';
  const isCondition = type === 'condition';

  return (
    <div className={cn(
      "relative group min-w-[220px] bg-card rounded-2xl border-2 transition-all duration-200 shadow-lg",
      selected ? "border-primary ring-4 ring-primary/10 scale-[1.02]" : "border-border hover:border-primary/50",
    )}>
      <Handle type="target" position={Position.Left} className="w-3 h-3 !bg-primary border-2 border-background" />
      
      <div className="p-4">
        <div className="flex items-center justify-between mb-3">
          <div className={cn(
            "p-2 rounded-xl",
            isAirflow ? "bg-blue-500/10 text-blue-500" :
            isSQL ? "bg-green-500/10 text-green-500" :
            isCondition ? "bg-purple-500/10 text-purple-500" :
            "bg-yellow-500/10 text-yellow-500"
          )}>
            <NodeIcon type={type} />
          </div>
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground/60 bg-muted px-2 py-1 rounded-md">
            {type.split('_')[0]}
          </div>
        </div>

        <div className="space-y-1">
          <div className="font-bold text-sm tracking-tight truncate">{data.label}</div>
          <div className="text-[11px] text-muted-foreground truncate opacity-70">
            {isAirflow ? `DAG: ${data.config?.dagId || 'unset'}` :
             isSQL ? 'Database Query' : 
             isCondition ? `Threshold: ${data.config?.threshold || 100}` :
             'Custom Script'}
          </div>
          {isAirflow && data.config?.logAssertion && (
            <div className="text-[9px] text-blue-500/70 font-mono truncate mt-1">
              Assert: "{data.config.logAssertion}"
            </div>
          )}
        </div>

        <div className="mt-4 flex items-center justify-between">
          <div className="flex -space-x-1">
            {[1, 2].map((i) => (
              <div key={i} className="w-5 h-5 rounded-full border-2 border-card bg-muted flex items-center justify-center text-[8px] font-bold">
                {i}
              </div>
            ))}
          </div>
          <ChevronRight className="w-4 h-4 text-muted-foreground/30 group-hover:text-primary transition-colors" />
        </div>
      </div>

      <Handle type="source" position={Position.Right} className="w-3 h-3 !bg-primary border-2 border-background" />
    </div>
  );
};

const nodeTypes = {
  airflow_trigger: CustomNode,
  sql_query: CustomNode,
  python_script: CustomNode,
  condition: CustomNode,
};

export default function WorkflowEditor() {
  const [, params] = useRoute('/workflows/:id');
  const id = parseInt(params?.id || '0');
  
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [prompt, setPrompt] = useState("");
  const [exportedCode, setExportedCode] = useState<string | null>(null);
  const { toast } = useToast();

  const { data: workflow, isLoading } = useWorkflow(id);
  const { mutateAsync: updateWorkflow } = useUpdateWorkflow();
  const { mutateAsync: generateWorkflow, isPending: isGenerating } = useGenerateWorkflow();
  const { mutateAsync: executeWorkflow, isPending: isStarting } = useExecuteWorkflow();

  const handleExport = async () => {
    try {
      const res = await fetch(`/api/workflows/${id}/export`);
      const data = await res.json();
      setExportedCode(data.code);
    } catch (e) {
      toast({ title: "Export Failed", variant: "destructive" });
    }
  };

  const downloadExport = () => {
    if (!exportedCode) return;
    const blob = new Blob([exportedCode], { type: 'text/plain' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `workflow_${id}.py`;
    a.click();
    toast({ title: "Exported", description: "Workflow exported as Python code" });
  };

  const [lastExecutionId, setLastExecutionId] = useState<number | null>(null);
  const { data: execution } = useExecution(lastExecutionId);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [logSearchQuery, setLogSearchQuery] = useState("");

  const selectedNode = nodes.find(n => n.id === selectedNodeId);

  const filteredLogs = (execution?.logs as any[])?.filter(log => {
    if (!logSearchQuery) return true;
    const message = typeof log === 'string' ? log : log.message;
    return message.toLowerCase().includes(logSearchQuery.toLowerCase());
  }) || [];

  const updateNodeData = (nodeId: string, newData: any) => {
    setNodes((nds) =>
      nds.map((node) => {
        if (node.id === nodeId) {
          return { ...node, data: { ...node.data, ...newData } };
        }
        return node;
      })
    );
  };

  // Sync with DB
  useEffect(() => {
    if (workflow) {
      setNodes(workflow.nodes as any || []);
      setEdges(workflow.edges as any || []);
    }
  }, [workflow, setNodes, setEdges]);

  const onConnect = useCallback((params: Edge | Connection) => setEdges((eds) => addEdge(params, eds)), [setEdges]);

  const handleSave = async () => {
    await updateWorkflow({ id, nodes, edges });
  };

  const handleGenerate = async () => {
    if (!prompt.trim()) return;
    try {
      const result = await generateWorkflow(prompt);
      setNodes(result.nodes);
      setEdges(result.edges);
      setPrompt("");
      toast({ title: "Generated", description: "Workflow generated from prompt" });
    } catch (e) {
      // hook handles error
    }
  };

  const handleRun = async () => {
    try {
      // Auto-save before run
      await handleSave();
      const exec = await executeWorkflow(id);
      setLastExecutionId(exec.id);
    } catch (e) {
      // handled
    }
  };

  if (isLoading) return <div className="flex items-center justify-center h-full">Loading editor...</div>;

  return (
    <div className="h-[calc(100vh-2rem)] flex flex-col bg-background/50 rounded-2xl border border-border overflow-hidden relative">
      {/* Header Toolbar */}
      <div className="h-16 border-b border-border flex items-center justify-between px-6 bg-card">
        <div>
          <h1 className="font-bold text-lg">{workflow?.name}</h1>
          <p className="text-xs text-muted-foreground">Visual Editor</p>
        </div>
        
        <div className="flex items-center gap-2 bg-muted/30 p-1 rounded-lg border border-border/50 max-w-md w-full mx-4">
          <Sparkles className="w-4 h-4 ml-2 text-purple-400 animate-pulse" />
          <Input 
            className="border-none bg-transparent h-8 focus-visible:ring-0 placeholder:text-muted-foreground/50" 
            placeholder="Describe workflow to generate..." 
            value={prompt}
            onChange={e => setPrompt(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleGenerate()}
          />
          <Button 
            size="sm" 
            variant="ghost" 
            className="h-8 w-8 p-0" 
            onClick={handleGenerate}
            disabled={isGenerating}
          >
            {isGenerating ? <Loader2 className="w-4 h-4 animate-spin" /> : <ArrowRightIcon className="w-4 h-4" />}
          </Button>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={handleExport}>
            <Terminal className="w-4 h-4 mr-2" />
            Export Python
          </Button>
          <Button variant="outline" size="sm" onClick={handleSave}>
            <Save className="w-4 h-4 mr-2" />
            Save
          </Button>
          <Button size="sm" onClick={handleRun} disabled={isStarting}>
            {isStarting ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Play className="w-4 h-4 mr-2" />}
            Run
          </Button>
        </div>
      </div>

      {/* Canvas */}
      <div className="flex-1 relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeClick={(_, node) => setSelectedNodeId(node.id)}
          onPaneClick={() => setSelectedNodeId(null)}
          nodeTypes={nodeTypes}
          fitView
          attributionPosition="bottom-right"
          className="bg-background"
        >
          <Background color="#333" gap={20} size={1} />
          <Controls className="bg-card border-border" />
          <MiniMap 
            className="bg-card border-border" 
            maskColor="rgba(0, 0, 0, 0.2)"
            nodeColor={(n) => {
              if (n.type === 'airflow_trigger') return '#60a5fa';
              if (n.type === 'sql_query') return '#4ade80';
              return '#a78bfa';
            }}
          />
          
          <Panel position="top-left" className="bg-card/80 backdrop-blur border border-border p-2 rounded-lg text-xs space-y-2">
            <div className="font-semibold text-muted-foreground mb-1">Node Types</div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Cloud className="w-3 h-3 text-blue-400" /> Airflow Trigger
            </div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Database className="w-3 h-3 text-green-400" /> SQL Query
            </div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Terminal className="w-3 h-3 text-yellow-400" /> Python Script
            </div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <GitBranch className="w-3 h-3 text-purple-400" /> Condition
            </div>
          </Panel>
        </ReactFlow>

        <Dialog open={!!exportedCode} onOpenChange={(open) => !open && setExportedCode(null)}>
          <DialogContent className="max-w-4xl max-h-[80vh] flex flex-col">
            <DialogHeader>
              <DialogTitle>Exported Python Code</DialogTitle>
            </DialogHeader>
            <div className="flex-1 overflow-auto bg-muted p-4 rounded-md font-mono text-xs whitespace-pre">
              {exportedCode}
            </div>
            <div className="flex justify-end gap-2 pt-4">
              <Button variant="outline" onClick={() => setExportedCode(null)}>
                Close
              </Button>
              <Button onClick={downloadExport}>
                <Download className="w-4 h-4 mr-2" />
                Download .py File
              </Button>
            </div>
          </DialogContent>
        </Dialog>

        <Sheet open={!!selectedNodeId} onOpenChange={(open) => !open && setSelectedNodeId(null)}>
          <SheetContent>
            <SheetHeader>
              <SheetTitle>Edit Node: {selectedNode?.data.label}</SheetTitle>
            </SheetHeader>
            <div className="py-6 space-y-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">Label</label>
                <Input 
                  value={selectedNode?.data.label || ''} 
                  onChange={(e) => updateNodeData(selectedNode!.id, { label: e.target.value })}
                />
              </div>

              {selectedNode?.data.type === 'airflow_trigger' && (
                <>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">DAG ID</label>
                    <Input 
                      value={selectedNode?.data.config?.dagId || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, dagId: e.target.value } 
                      })}
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Log Assertion (Pattern to find)</label>
                    <Input 
                      placeholder="e.g. SUCCESS: All records processed"
                      value={selectedNode?.data.config?.logAssertion || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, logAssertion: e.target.value } 
                      })}
                    />
                  </div>
                </>
              )}

              {selectedNode?.data.type === 'sql_query' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">SQL Query</label>
                  <textarea 
                    className="flex min-h-[100px] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    value={selectedNode?.data.config?.query || ''} 
                    onChange={(e) => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, query: e.target.value } 
                    })}
                  />
                </div>
              )}

              {selectedNode?.data.type === 'python_script' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">Python Code</label>
                  <textarea 
                    className="flex min-h-[150px] w-full rounded-md border border-input bg-transparent px-3 py-2 font-mono text-xs shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    value={selectedNode?.data.config?.code || ''} 
                    onChange={(e) => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, code: e.target.value } 
                    })}
                  />
                </div>
              )}

              {selectedNode?.data.type === 'condition' && (
                <div className="space-y-2">
                  <label className="text-sm font-medium">Threshold (Count &gt; X)</label>
                  <Input 
                    type="number"
                    value={selectedNode?.data.config?.threshold || 100} 
                    onChange={(e) => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, threshold: parseInt(e.target.value) } 
                    })}
                  />
                </div>
              )}
              
              <div className="pt-4">
                <Button className="w-full" onClick={handleSave}>
                  <Save className="w-4 h-4 mr-2" />
                  Save Changes
                </Button>
              </div>
            </div>
          </SheetContent>
        </Sheet>

        {/* Live Execution Logs Overlay */}
        {execution && (
          <div className="absolute bottom-0 left-0 right-0 h-64 bg-card border-t border-border shadow-[0_-4px_20px_rgba(0,0,0,0.5)] flex flex-col transition-transform duration-300">
            <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-muted/20">
              <div className="flex items-center gap-3 flex-1">
                <div className={cn("w-2 h-2 rounded-full", 
                  execution.status === 'running' ? 'bg-blue-500 animate-pulse' : 
                  execution.status === 'completed' ? 'bg-green-500' : 'bg-red-500'
                )} />
                <span className="font-mono text-sm font-medium shrink-0">Execution #{execution.id}</span>
                
                <div className="relative max-w-xs w-full ml-4">
                  <SearchIcon className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-muted-foreground" />
                  <Input 
                    className="h-7 pl-7 text-[10px] bg-background/50" 
                    placeholder="Search logs..." 
                    value={logSearchQuery}
                    onChange={e => setLogSearchQuery(e.target.value)}
                  />
                </div>
              </div>
              <button 
                onClick={() => setLastExecutionId(null)} 
                className="text-xs text-muted-foreground hover:text-foreground ml-4"
              >
                Close
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-4 font-mono text-xs space-y-1">
              {filteredLogs.length > 0 ? (
                filteredLogs.map((log, i) => (
                  <div key={i} className="flex gap-2 text-muted-foreground/80">
                    <span className="text-muted-foreground/40 shrink-0">
                      {log.timestamp ? format(new Date(log.timestamp), 'HH:mm:ss.SSS') : '>'}
                    </span>
                    <span className={cn(
                      log.level === 'error' ? 'text-red-400' : 
                      log.level === 'warn' ? 'text-yellow-400' : 
                      log.level === 'debug' ? 'text-blue-400/70' : 'text-foreground'
                    )}>
                      {typeof log === 'string' ? log : log.message}
                    </span>
                  </div>
                ))
              ) : (
                <div className="text-muted-foreground/30 italic text-center py-4">
                  {logSearchQuery ? `No logs matching "${logSearchQuery}"` : "Waiting for logs..."}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
