import { useState, useCallback, useMemo, useEffect } from "react";
import { AppLayout } from "@/components/layout/AppLayout";
import { useLocation, useRoute } from "wouter";
import { useWorkflow, useCreateWorkflow, useUpdateWorkflow, useRunWorkflow } from "@/hooks/use-workflows";
import { 
  ReactFlow, 
  Controls, 
  Background, 
  useNodesState, 
  useEdgesState, 
  addEdge,
  Connection,
  Edge,
  Node,
  ReactFlowProvider,
  BackgroundVariant
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import CustomNode from "@/components/workflow/CustomNode";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { 
  Play, 
  Save, 
  ArrowLeft, 
  Code2, 
  Database, 
  CheckCircle2, 
  Server,
  Settings,
  MoreVertical
} from "lucide-react";
import Editor from "@monaco-editor/react";
import jsYaml from "js-yaml";
import { useToast } from "@/hooks/use-toast";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const nodeTypes = {
  custom: CustomNode,
};

type StepType = 'sql' | 'script' | 'assertion' | 'api';

const initialNodes: Node[] = [
  { 
    id: 'trigger', 
    type: 'custom', 
    position: { x: 250, y: 50 }, 
    data: { label: 'Start', type: 'trigger', description: 'Manual Trigger' } 
  },
];

export default function WorkflowBuilder() {
  const [match, params] = useRoute("/workflows/:id");
  const isNew = params?.id === "new";
  const id = params?.id ? parseInt(params.id) : 0;
  
  const [location, setLocation] = useLocation();
  const { data: workflow, isLoading } = useWorkflow(id);
  const createMutation = useCreateWorkflow();
  const updateMutation = useUpdateWorkflow();
  const runMutation = useRunWorkflow();
  const { toast } = useToast();

  const [name, setName] = useState("New Workflow");
  const [description, setDescription] = useState("");
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [showCode, setShowCode] = useState(false);

  // Load workflow data
  useEffect(() => {
    if (workflow) {
      setName(workflow.name);
      setDescription(workflow.description || "");
      try {
        const definition = JSON.parse(workflow.definition);
        if (definition.nodes && definition.edges) {
          setNodes(definition.nodes);
          setEdges(definition.edges);
        }
      } catch (e) {
        console.error("Failed to parse workflow definition", e);
      }
    }
  }, [workflow, setNodes, setEdges]);

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges],
  );

  const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  const addNode = (type: StepType) => {
    const newNode: Node = {
      id: `${type}-${Date.now()}`,
      type: 'custom',
      position: { 
        x: 250 + (nodes.length * 20), 
        y: 150 + (nodes.length * 50) 
      },
      data: { 
        label: `New ${type} step`, 
        type,
        config: {} 
      },
    };
    setNodes((nds) => [...nds, newNode]);
  };

  const handleSave = async () => {
    const definition = JSON.stringify({ nodes, edges });
    const payload = { name, description, definition };

    if (isNew) {
      const newWorkflow = await createMutation.mutateAsync(payload as any);
      setLocation(`/workflows/${newWorkflow.id}`);
    } else {
      await updateMutation.mutateAsync({ id, ...payload });
    }
  };

  const handleRun = async () => {
    if (isNew) {
      toast({ title: "Save first", description: "Please save the workflow before running.", variant: "destructive" });
      return;
    }
    await runMutation.mutateAsync({ id });
  };

  const updateNodeData = (key: string, value: any) => {
    if (!selectedNode) return;
    setNodes((nds) =>
      nds.map((node) => {
        if (node.id === selectedNode.id) {
          const newData = { ...node.data, [key]: value };
          // Update selected node reference to keep inputs controlled
          setSelectedNode({ ...node, data: newData }); 
          return { ...node, data: newData };
        }
        return node;
      })
    );
  };

  // Generate YAML representation for the code view
  const yamlContent = useMemo(() => {
    try {
      return jsYaml.dump({ 
        name, 
        description, 
        steps: nodes.map(n => ({ id: n.id, type: n.data.type, ...n.data.config })) 
      });
    } catch (e) {
      return "";
    }
  }, [nodes, name, description]);

  if (!isNew && isLoading) return <div>Loading...</div>;

  return (
    <div className="flex flex-col h-screen bg-background">
      {/* Header */}
      <header className="flex items-center justify-between border-b bg-card px-6 py-3 shadow-sm z-10">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => setLocation('/workflows')}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <Input 
              value={name} 
              onChange={(e) => setName(e.target.value)} 
              className="h-8 w-[300px] border-transparent bg-transparent px-0 font-display text-lg font-bold hover:bg-accent/50 focus:bg-accent focus:px-2 transition-all"
            />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <div className="mr-4 flex gap-1 bg-muted p-1 rounded-lg">
            <Button 
              variant={!showCode ? "secondary" : "ghost"} 
              size="sm" 
              onClick={() => setShowCode(false)}
              className="h-7 text-xs"
            >
              Visual
            </Button>
            <Button 
              variant={showCode ? "secondary" : "ghost"} 
              size="sm" 
              onClick={() => setShowCode(true)}
              className="h-7 text-xs"
            >
              Code
            </Button>
          </div>
          <Button 
            variant="outline" 
            size="sm"
            onClick={handleRun} 
            disabled={runMutation.isPending || isNew}
            className="gap-2"
          >
            <Play className="h-4 w-4 text-green-500" /> 
            Run
          </Button>
          <Button 
            size="sm"
            onClick={handleSave} 
            disabled={createMutation.isPending || updateMutation.isPending}
            className="gap-2 bg-primary hover:bg-primary/90 shadow-lg shadow-primary/20"
          >
            <Save className="h-4 w-4" /> 
            Save
          </Button>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar for dragging nodes */}
        {!showCode && (
          <aside className="w-64 border-r bg-card/50 p-4 flex flex-col gap-4 overflow-y-auto">
            <div className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-2">Components</div>
            
            <div className="space-y-3">
              <Button variant="outline" className="w-full justify-start gap-3 h-auto py-3" onClick={() => addNode('sql')}>
                <div className="p-1.5 bg-blue-500/10 rounded-md text-blue-500"><Database className="h-4 w-4" /></div>
                <div className="text-left">
                  <div className="font-medium">SQL Query</div>
                  <div className="text-xs text-muted-foreground font-normal">Execute database query</div>
                </div>
              </Button>

              <Button variant="outline" className="w-full justify-start gap-3 h-auto py-3" onClick={() => addNode('api')}>
                <div className="p-1.5 bg-purple-500/10 rounded-md text-purple-500"><Server className="h-4 w-4" /></div>
                <div className="text-left">
                  <div className="font-medium">API Request</div>
                  <div className="text-xs text-muted-foreground font-normal">HTTP/REST call</div>
                </div>
              </Button>

              <Button variant="outline" className="w-full justify-start gap-3 h-auto py-3" onClick={() => addNode('script')}>
                <div className="p-1.5 bg-yellow-500/10 rounded-md text-yellow-500"><Code2 className="h-4 w-4" /></div>
                <div className="text-left">
                  <div className="font-medium">Script</div>
                  <div className="text-xs text-muted-foreground font-normal">Node.js/Python script</div>
                </div>
              </Button>

              <Button variant="outline" className="w-full justify-start gap-3 h-auto py-3" onClick={() => addNode('assertion')}>
                <div className="p-1.5 bg-green-500/10 rounded-md text-green-500"><CheckCircle2 className="h-4 w-4" /></div>
                <div className="text-left">
                  <div className="font-medium">Assertion</div>
                  <div className="text-xs text-muted-foreground font-normal">Validate data quality</div>
                </div>
              </Button>
            </div>
          </aside>
        )}

        {/* Editor Area */}
        <div className="flex-1 relative bg-muted/20">
          {showCode ? (
             <Editor
               height="100%"
               defaultLanguage="yaml"
               value={yamlContent}
               theme="vs-dark"
               options={{
                 minimap: { enabled: false },
                 fontSize: 14,
                 padding: { top: 20 },
                 scrollBeyondLastLine: false,
               }}
             />
          ) : (
            <ReactFlowProvider>
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onNodeClick={onNodeClick}
                onPaneClick={onPaneClick}
                nodeTypes={nodeTypes}
                fitView
                className="bg-muted/20"
              >
                <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
                <Controls />
              </ReactFlow>
            </ReactFlowProvider>
          )}
        </div>

        {/* Property Panel */}
        {selectedNode && !showCode && (
          <div className="w-80 border-l bg-card shadow-xl flex flex-col h-full animate-in slide-in-from-right duration-200">
            <div className="p-4 border-b flex items-center justify-between">
              <h3 className="font-semibold">Properties</h3>
              <Button variant="ghost" size="icon" size="sm" onClick={() => setSelectedNode(null)}>
                <Settings className="h-4 w-4" />
              </Button>
            </div>
            
            <div className="p-4 space-y-6 overflow-y-auto flex-1">
              <div className="space-y-2">
                <Label>Step Name</Label>
                <Input 
                  value={selectedNode.data.label as string} 
                  onChange={(e) => updateNodeData('label', e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <Label>Description</Label>
                <Textarea 
                  value={selectedNode.data.description as string || ''} 
                  onChange={(e) => updateNodeData('description', e.target.value)}
                  placeholder="Describe this step..."
                  className="h-20 resize-none"
                />
              </div>

              {/* Dynamic properties based on type */}
              {selectedNode.data.type === 'sql' && (
                <div className="space-y-2">
                  <Label>SQL Query</Label>
                  <Textarea 
                    className="font-mono text-xs h-32 bg-muted/50" 
                    placeholder="SELECT * FROM users..."
                    value={(selectedNode.data.config as any)?.query || ''}
                    onChange={(e) => {
                      const config = selectedNode.data.config as any || {};
                      updateNodeData('config', { ...config, query: e.target.value });
                    }}
                  />
                </div>
              )}

              {selectedNode.data.type === 'api' && (
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label>Method</Label>
                    <select 
                      className="w-full rounded-md border bg-transparent px-3 py-1 shadow-sm"
                      value={(selectedNode.data.config as any)?.method || 'GET'}
                      onChange={(e) => {
                        const config = selectedNode.data.config as any || {};
                        updateNodeData('config', { ...config, method: e.target.value });
                      }}
                    >
                      <option value="GET">GET</option>
                      <option value="POST">POST</option>
                      <option value="PUT">PUT</option>
                      <option value="DELETE">DELETE</option>
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label>URL</Label>
                    <Input 
                      placeholder="https://api.example.com/v1/resource"
                      value={(selectedNode.data.config as any)?.url || ''}
                      onChange={(e) => {
                        const config = selectedNode.data.config as any || {};
                        updateNodeData('config', { ...config, url: e.target.value });
                      }}
                    />
                  </div>
                </div>
              )}
            </div>
            
            <div className="p-4 border-t bg-muted/30">
              <Button 
                variant="destructive" 
                className="w-full"
                onClick={() => {
                  setNodes((nds) => nds.filter(n => n.id !== selectedNode.id));
                  setSelectedNode(null);
                }}
              >
                Delete Step
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
