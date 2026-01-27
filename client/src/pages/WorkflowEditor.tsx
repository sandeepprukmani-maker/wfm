import { useCallback, useEffect, useState, useMemo } from 'react';
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
import { useCredentials } from '@/hooks/use-credentials';
import { useExecution } from '@/hooks/use-executions';
import { useRoute } from 'wouter';
import { apiRequest } from '@/lib/queryClient';
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
  Download,
  Plus,
  Trash2,
  Globe,
  Wand2,
  Clock
} from 'lucide-react';

const TIMEZONES = [
  { value: 'UTC', label: 'UTC' },
  { value: 'America/New_York', label: 'Eastern Time (US)' },
  { value: 'America/Chicago', label: 'Central Time (US)' },
  { value: 'America/Denver', label: 'Mountain Time (US)' },
  { value: 'America/Los_Angeles', label: 'Pacific Time (US)' },
  { value: 'America/Anchorage', label: 'Alaska Time' },
  { value: 'Pacific/Honolulu', label: 'Hawaii Time' },
  { value: 'Europe/London', label: 'London (GMT/BST)' },
  { value: 'Europe/Paris', label: 'Paris (CET/CEST)' },
  { value: 'Europe/Berlin', label: 'Berlin (CET/CEST)' },
  { value: 'Asia/Tokyo', label: 'Tokyo (JST)' },
  { value: 'Asia/Shanghai', label: 'Shanghai (CST)' },
  { value: 'Asia/Kolkata', label: 'India (IST)' },
  { value: 'Asia/Dubai', label: 'Dubai (GST)' },
  { value: 'Asia/Singapore', label: 'Singapore (SGT)' },
  { value: 'Australia/Sydney', label: 'Sydney (AEST/AEDT)' },
  { value: 'Australia/Perth', label: 'Perth (AWST)' },
  { value: 'Pacific/Auckland', label: 'Auckland (NZST)' },
];
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useToast } from '@/hooks/use-toast';
import { cn } from '@/lib/utils';
import { format } from 'date-fns';
import { Handle, Position } from 'reactflow';

const NodeIcon = ({ type, className }: { type: string, className?: string }) => {
  switch(type) {
    case 'airflow_trigger': return <Cloud className={cn("w-5 h-5", className)} />;
    case 'airflow_log_check': return <SearchIcon className={cn("w-5 h-5", className)} />;
    case 'sql_query': return <Database className={cn("w-5 h-5", className)} />;
    case 'python_script': return <Terminal className={cn("w-5 h-5", className)} />;
    case 'condition': return <GitBranch className={cn("w-5 h-5", className)} />;
    case 'api_request': return <Globe className={cn("w-5 h-5", className)} />;
    case 's3_operation': return <Database className={cn("w-5 h-5", className)} />;
    case 'sftp_operation': return <Globe className={cn("w-5 h-5", className)} />;
    default: return <Sparkles className={cn("w-5 h-5", className)} />;
  }
};

const CustomNode = ({ id, data, type, selected, execution }: any) => {
  const nodeStatus = (execution?.results as any)?.[id]?.status;

  const isAirflowTrigger = type === 'airflow_trigger';
  const isAirflowLog = type === 'airflow_log_check';
  const isSQL = type === 'sql_query';
  const isPython = type === 'python_script';
  const isCondition = type === 'condition';
  const isAPI = type === 'api_request';

  const statusColors = {
    running: "border-yellow-500 shadow-[0_0_15px_rgba(234,179,8,0.3)]",
    success: "border-green-500 shadow-[0_0_15px_rgba(34,197,94,0.3)]",
    failure: "border-red-500 shadow-[0_0_15px_rgba(239,68,68,0.3)]",
  };

  const statusBg = {
    running: "bg-yellow-500/10",
    success: "bg-green-500/10",
    failure: "bg-red-500/10",
  };

  return (
    <div className={cn(
      "relative group min-w-[220px] bg-card rounded-2xl border-2 transition-all duration-200 shadow-lg",
      selected ? "border-primary ring-4 ring-primary/10 scale-[1.02]" : "border-border hover:border-primary/50",
      nodeStatus && statusColors[nodeStatus as keyof typeof statusColors]
    )}>
      {nodeStatus === 'running' && (
        <div className="absolute -top-3 -right-3">
          <Loader2 className="w-6 h-6 text-yellow-500 animate-spin" />
        </div>
      )}
      <Handle type="target" position={Position.Left} className="w-3 h-3 !bg-primary border-2 border-background" />
      
      <div className="p-4">
        <div className="flex items-center justify-between mb-3">
          <div className={cn(
            "p-2 rounded-xl",
            nodeStatus ? statusBg[nodeStatus as keyof typeof statusBg] :
            isAirflowTrigger || isAirflowLog ? "bg-blue-500/10 text-blue-500" :
            isSQL ? "bg-green-500/10 text-green-500" :
            isCondition ? "bg-purple-500/10 text-purple-500" :
            isAPI ? "bg-orange-500/10 text-orange-500" :
            "bg-yellow-500/10 text-yellow-500"
          )}>
            <NodeIcon type={type} className={cn(
              nodeStatus === 'running' && "text-yellow-500",
              nodeStatus === 'success' && "text-green-500",
              nodeStatus === 'failure' && "text-red-500"
            )} />
          </div>
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground/60 bg-muted px-2 py-1 rounded-md">
            {type.split('_')[0]}
          </div>
        </div>

        <div className="space-y-1">
          <div className="font-bold text-sm tracking-tight truncate">{data.label}</div>
          <div className="text-[11px] text-muted-foreground truncate opacity-70">
            {isAirflowTrigger ? `Trigger DAG: ${data.config?.dagId || 'unset'}` :
             isAirflowLog ? `Check Log: ${data.config?.logAssertion || 'unset'}` :
             isSQL ? 'Database Query' : 
             isCondition ? `Variable: ${data.config?.variable || 'lastRecordCount'}` :
             isAPI ? `${data.config?.method || 'GET'} ${data.config?.url || 'URL unset'}` :
             'Custom Script'}
          </div>
          {isAirflowLog && data.config?.logAssertion && (
            <div className="text-[9px] text-blue-500/70 font-mono truncate border-l-2 border-blue-500/30 pl-1 mt-1">
              {data.config.taskName ? `${data.config.taskName}: ` : ''}"{data.config.logAssertion}"
            </div>
          )}
          {data.config?.scheduledTime && data.config?.timezone && (
            <div className="flex items-center gap-1 text-[9px] text-blue-400 bg-blue-500/10 px-1.5 py-0.5 rounded mt-1 w-fit">
              <Clock className="w-2.5 h-2.5" />
              {data.config.scheduledTime} ({data.config.timezone.split('/').pop()?.replace('_', ' ')})
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

      {isCondition ? (
        <>
          <div className="absolute -right-12 top-[30%] -translate-y-1/2 flex items-center gap-2">
            <span className="text-[10px] font-bold text-green-500 bg-green-500/10 px-1 rounded">TRUE</span>
            <Handle 
              type="source" 
              position={Position.Right} 
              id="success"
              className="w-4 h-4 !bg-green-500 border-2 border-background hover:scale-125 transition-transform" 
              style={{ top: 'auto', right: -4 }}
            />
          </div>
          <div className="absolute -right-12 top-[70%] -translate-y-1/2 flex items-center gap-2">
            <span className="text-[10px] font-bold text-red-500 bg-red-500/10 px-1 rounded">FALSE</span>
            <Handle 
              type="source" 
              position={Position.Right} 
              id="failure"
              className="w-4 h-4 !bg-red-500 border-2 border-background hover:scale-125 transition-transform" 
              style={{ top: 'auto', right: -4 }}
            />
          </div>
        </>
      ) : (
        <Handle type="source" position={Position.Right} className="w-3 h-3 !bg-primary border-2 border-background" />
      )}
    </div>
  );
};

  const nodeTypes = {
    airflow_trigger: CustomNode,
    airflow_log_check: CustomNode,
    sql_query: CustomNode,
    python_script: CustomNode,
    condition: CustomNode,
    api_request: CustomNode,
    s3_operation: CustomNode,
    sftp_operation: CustomNode,
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
  const { data: credentials } = useCredentials();
  const { mutateAsync: updateWorkflow } = useUpdateWorkflow();
  const { mutateAsync: generateWorkflow, isPending: isGenerating } = useGenerateWorkflow();
  const { mutateAsync: executeWorkflow, isPending: isStarting } = useExecuteWorkflow();

  const airflowCredentials = credentials?.filter(c => c.type === 'airflow') || [];

  const handleExport = async () => {
    try {
      const res = await fetch(`/api/workflows/${id}/export`);
      if (!res.ok) throw new Error("Export request failed");
      const data = await res.json();
      setExportedCode(data.code);
    } catch (e) {
      toast({ title: "Export Failed", variant: "destructive", description: "Could not generate Python code. Ensure the backend is running." });
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

  // Load latest execution for this workflow if none selected
  useEffect(() => {
    if (workflow?.lastPrompt) {
      setPrompt(workflow.lastPrompt);
    }
  }, [workflow]);

  useEffect(() => {
    const searchParams = new URLSearchParams(window.location.search);
    const execId = searchParams.get('executionId');
    if (execId) {
      setLastExecutionId(parseInt(execId));
    }
  }, []);

  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [logSearchQuery, setLogSearchQuery] = useState("");

  const selectedNode = nodes.find(n => n.id === selectedNodeId);

  const nodeTypes = useMemo(() => ({
    airflow_trigger: (props: any) => <CustomNode {...props} execution={execution} />,
    airflow_log_check: (props: any) => <CustomNode {...props} execution={execution} />,
    sql_query: (props: any) => <CustomNode {...props} execution={execution} />,
    python_script: (props: any) => <CustomNode {...props} execution={execution} />,
    condition: (props: any) => <CustomNode {...props} execution={execution} />,
    api_request: (props: any) => <CustomNode {...props} execution={execution} />,
    s3_operation: (props: any) => <CustomNode {...props} execution={execution} />,
    sftp_operation: (props: any) => <CustomNode {...props} execution={execution} />,
  }), [execution]);

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

  const onConnect = useCallback((params: Edge | Connection) => {
    let edgeParams = { ...params };
    
    // Add styling for conditional branches
    if (params.sourceHandle === 'success') {
      edgeParams = {
        ...edgeParams,
        label: 'TRUE',
        style: { stroke: '#22c55e', strokeWidth: 2 },
        labelStyle: { fill: '#22c55e', fontWeight: 700, fontSize: 10 }
      } as any;
    } else if (params.sourceHandle === 'failure') {
      edgeParams = {
        ...edgeParams,
        label: 'FALSE',
        style: { stroke: '#ef4444', strokeWidth: 2 },
        labelStyle: { fill: '#ef4444', fontWeight: 700, fontSize: 10 }
      } as any;
    }
    
    setEdges((eds) => addEdge(edgeParams, eds));
  }, [setEdges]);

  const handleSave = async () => {
    await updateWorkflow({ id, nodes, edges });
  };

  const handleGenerate = async () => {
    if (!prompt.trim()) return;
    try {
      const result = await generateWorkflow({ prompt, workflowId: id });
      setNodes(result.nodes);
      setEdges(result.edges);
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
          edges={edges.map(edge => {
            if (edge.sourceHandle === 'success') {
              return {
                ...edge,
                label: 'TRUE',
                style: { stroke: '#22c55e', strokeWidth: 2 },
                labelStyle: { fill: '#22c55e', fontWeight: 700, fontSize: 10 }
              };
            }
            if (edge.sourceHandle === 'failure') {
              return {
                ...edge,
                label: 'FALSE',
                style: { stroke: '#ef4444', strokeWidth: 2 },
                labelStyle: { fill: '#ef4444', fontWeight: 700, fontSize: 10 }
              };
            }
            return edge;
          })}
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
              <SearchIcon className="w-3 h-3 text-blue-400" /> Airflow Log Check
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
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Globe className="w-3 h-3 text-orange-400" /> API Request
            </div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Database className="w-3 h-3 text-cyan-400" /> S3 Operation
            </div>
            <div className="flex items-center gap-2 cursor-grab active:cursor-grabbing hover:bg-muted p-1 rounded">
              <Globe className="w-3 h-3 text-indigo-400" /> SFTP Operation
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
                <div className="space-y-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Credential</label>
                    <Select 
                      value={selectedNode?.data.config?.credentialId?.toString() || ""}
                      onValueChange={(val) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, credentialId: parseInt(val) } 
                      })}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select Airflow Credential" />
                      </SelectTrigger>
                      <SelectContent>
                        {airflowCredentials.map(c => (
                          <SelectItem key={c.id} value={c.id.toString()}>{c.name}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
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
                    <label className="text-sm font-medium">Configuration (JSON)</label>
                    <textarea 
                      className="flex min-h-[80px] w-full rounded-md border border-input bg-transparent px-3 py-2 font-mono text-xs shadow-sm"
                      placeholder='e.g. {"key": "value"}'
                      value={selectedNode?.data.config?.conf ? JSON.stringify(selectedNode.data.config.conf, null, 2) : ''} 
                      onChange={(e) => {
                        try {
                          const conf = JSON.parse(e.target.value);
                          updateNodeData(selectedNode!.id, { config: { ...selectedNode.data.config, conf } });
                        } catch {}
                      }}
                    />
                  </div>
                  
                  <div className="pt-2 border-t space-y-2">
                    <label className="text-[10px] uppercase font-bold text-muted-foreground">Manual Actions</label>
                    <div className="grid grid-cols-2 gap-2">
                      <Button 
                        variant="outline" 
                        size="sm" 
                        className="text-[10px] h-8"
                        onClick={() => apiRequest("POST", "/api/airflow/mark-failed", { dagId: selectedNode.data.config.dagId })}
                      >
                        Mark Failed
                      </Button>
                      <Button 
                        variant="outline" 
                        size="sm" 
                        className="text-[10px] h-8"
                        onClick={() => apiRequest("POST", "/api/airflow/clear-task", { dagId: selectedNode.data.config.dagId })}
                      >
                        Clear Task
                      </Button>
                    </div>
                  </div>
                </div>
              )}

              {selectedNode?.data.type === 'airflow_log_check' && (
                <div className="space-y-3">
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Credential</label>
                    <Select 
                      value={selectedNode?.data.config?.credentialId?.toString() || ""}
                      onValueChange={(val) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, credentialId: parseInt(val) } 
                      })}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select Airflow Credential" />
                      </SelectTrigger>
                      <SelectContent>
                        {airflowCredentials.map(c => (
                          <SelectItem key={c.id} value={c.id.toString()}>{c.name}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">DAG ID (Optional)</label>
                    <Input 
                      placeholder="Inherits from trigger if empty"
                      value={selectedNode?.data.config?.dagId || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, dagId: e.target.value } 
                      })}
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Task ID (Optional)</label>
                    <Input 
                      placeholder="e.g. process_data"
                      value={selectedNode?.data.config?.taskName || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, taskName: e.target.value } 
                      })}
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Log Pattern</label>
                    <Input 
                      placeholder="e.g. Success: 5000 rows"
                      value={selectedNode?.data.config?.logAssertion || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, logAssertion: e.target.value } 
                      })}
                    />
                  </div>
                </div>
              )}

              {selectedNode?.data.type === 'sql_query' && (
                <div className="space-y-4">
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
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Python Assertion (Optional)</label>
                    <textarea 
                      placeholder="e.g. any(r['value'] > 100 for r in results)"
                      className="flex min-h-[60px] w-full rounded-md border border-input bg-transparent px-3 py-2 font-mono text-xs shadow-sm"
                      value={selectedNode?.data.config?.pythonAssertion || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, pythonAssertion: e.target.value } 
                      })}
                    />
                    <p className="text-[10px] text-muted-foreground italic">Use 'results' to access the row list.</p>
                  </div>
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

              {/* Schedule Section - Available for all node types */}
              <div className="pt-4 border-t space-y-3">
                <div className="flex items-center gap-2">
                  <Clock className="w-4 h-4 text-blue-400" />
                  <label className="text-sm font-semibold">Schedule (Optional)</label>
                </div>
                <p className="text-[11px] text-muted-foreground">
                  Set a specific time and timezone for this node to run.
                </p>
                <div className="space-y-2">
                  <label className="text-xs font-medium">Scheduled Time</label>
                  <Input 
                    type="time"
                    value={selectedNode?.data.config?.scheduledTime || ''} 
                    onChange={(e) => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, scheduledTime: e.target.value } 
                    })}
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-xs font-medium">Timezone</label>
                  <Select 
                    value={selectedNode?.data.config?.timezone || ""}
                    onValueChange={(val) => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, timezone: val } 
                    })}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select Timezone" />
                    </SelectTrigger>
                    <SelectContent>
                      {TIMEZONES.map(tz => (
                        <SelectItem key={tz.value} value={tz.value}>{tz.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                {selectedNode?.data.config?.scheduledTime && selectedNode?.data.config?.timezone && (
                  <div className="text-xs text-green-500 flex items-center gap-1 bg-green-500/10 px-2 py-1 rounded">
                    <Clock className="w-3 h-3" />
                    Scheduled: {selectedNode.data.config.scheduledTime} ({TIMEZONES.find(tz => tz.value === selectedNode.data.config.timezone)?.label || selectedNode.data.config.timezone})
                  </div>
                )}
                {(selectedNode?.data.config?.scheduledTime || selectedNode?.data.config?.timezone) && (
                  <Button 
                    variant="ghost" 
                    size="sm" 
                    className="text-xs text-muted-foreground"
                    onClick={() => updateNodeData(selectedNode!.id, { 
                      config: { ...selectedNode.data.config, scheduledTime: undefined, timezone: undefined } 
                    })}
                  >
                    Clear Schedule
                  </Button>
                )}
              </div>
              
              {selectedNode?.data.type === 'api_request' && (
                <div className="space-y-3">
                  <div className="space-y-1">
                    <label className="text-sm font-medium">URL</label>
                    <Input 
                      placeholder="https://api.example.com/v1/data"
                      value={selectedNode?.data.config?.url || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, url: e.target.value } 
                      })}
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Method</label>
                    <select 
                      className="w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm"
                      value={selectedNode?.data.config?.method || 'GET'} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, method: e.target.value } 
                      })}
                    >
                      {['GET', 'POST', 'PUT', 'DELETE', 'PATCH'].map(m => <option key={m} value={m}>{m}</option>)}
                    </select>
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Headers (JSON)</label>
                    <textarea 
                      className="flex min-h-[60px] w-full rounded-md border border-input bg-transparent px-3 py-2 font-mono text-xs"
                      value={selectedNode?.data.config?.headers ? JSON.stringify(selectedNode.data.config.headers, null, 2) : '{}'} 
                      onChange={(e) => {
                        try {
                          const headers = JSON.parse(e.target.value);
                          updateNodeData(selectedNode!.id, { config: { ...selectedNode.data.config, headers } });
                        } catch {}
                      }}
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm font-medium">Body</label>
                    <textarea 
                      className="flex min-h-[80px] w-full rounded-md border border-input bg-transparent px-3 py-2 font-mono text-xs"
                      value={selectedNode?.data.config?.body || ''} 
                      onChange={(e) => updateNodeData(selectedNode!.id, { 
                        config: { ...selectedNode.data.config, body: e.target.value } 
                      })}
                    />
                  </div>
                </div>
              )}

              <div className="pt-4 space-y-2">
                <div className="relative">
                  <Wand2 className="absolute left-2 top-1/2 -translate-y-1/2 w-4 h-4 text-purple-400" />
                  <Input 
                    placeholder="AI Refine (e.g. 'Set body to date-1')" 
                    className="pl-8 text-xs"
                    onKeyDown={async (e) => {
                      if (e.key === 'Enter') {
                        const instruction = (e.target as HTMLInputElement).value;
                        if (!instruction) return;
                        
                        try {
                          const res = await fetch(`/api/nodes/${selectedNode!.id}/refine`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ 
                              nodeId: selectedNode!.id, 
                              currentConfig: selectedNode!.data.config,
                              instruction 
                            })
                          });
                          const refined = await res.json();
                          updateNodeData(selectedNode!.id, { config: refined });
                          (e.target as HTMLInputElement).value = '';
                          toast({ title: "Refined", description: "AI updated node configuration" });
                        } catch {
                          toast({ title: "Refinement Failed", variant: "destructive" });
                        }
                      }
                    }}
                  />
                </div>
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
