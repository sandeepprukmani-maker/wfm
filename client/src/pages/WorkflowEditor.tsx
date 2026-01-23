import { useCallback, useRef, useState, useMemo, useEffect } from "react";
import ReactFlow, { 
  Background, 
  Controls, 
  ReactFlowProvider,
  addEdge,
  useNodesState,
  useEdgesState,
  Connection,
  Edge,
  Node,
  ReactFlowInstance,
  Panel
} from "reactflow";
import { useRoute } from "wouter";
import { Link } from "wouter";
import { ChevronLeft, Save, Play, Sparkles, LayoutList, Loader2, Zap } from "lucide-react";
import Sidebar from "@/components/Sidebar";
import PropertiesPanel from "@/components/PropertiesPanel";
import ExecutionsList from "@/components/ExecutionsList";
import AIGenerateModal from "@/components/AIGenerateModal";
import { TriggerNode, HttpNode, LogicNode, TransformNode, S3Node, DatabaseNode, AirflowNode } from "@/components/CustomNodes";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { useWorkflow, useUpdateWorkflow, useExecuteWorkflow } from "@/hooks/use-workflows";

const nodeTypes = {
  trigger: TriggerNode,
  http: HttpNode,
  logic: LogicNode,
  transform: TransformNode,
  s3: S3Node,
  db: DatabaseNode,
  airflow: AirflowNode,
};

function EditorContent({ id }: { id: number }) {
  const { data: workflow, isLoading } = useWorkflow(id);
  const updateWorkflow = useUpdateWorkflow();
  const executeWorkflow = useExecuteWorkflow();

  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [workflowName, setWorkflowName] = useState("");
  const [showAIModal, setShowAIModal] = useState(false);
  const [showLogs, setShowLogs] = useState(false);

  // Initialize workflow data
  useEffect(() => {
    if (workflow) {
      setNodes(workflow.nodes || []);
      setEdges(workflow.edges || []);
      setWorkflowName(workflow.name);
    }
  }, [workflow, setNodes, setEdges]);

  // Handle Connections
  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  // Handle Drag & Drop
  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow');
      if (typeof type === 'undefined' || !type) return;

      const position = reactFlowInstance?.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });

      const newNode: Node = {
        id: `${type}-${Date.now()}`,
        type,
        position: position || { x: 0, y: 0 },
        data: { label: `${type} Node` },
      };

      setNodes((nds) => nds.concat(newNode));
    },
    [reactFlowInstance, setNodes]
  );

  // Handle Node Selection
  const onNodeClick = useCallback((_: any, node: Node) => {
    setSelectedNode(node);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  // Handle Property Updates
  const onNodeUpdate = useCallback((nodeId: string, newData: any) => {
    if (newData._delete) {
      setNodes((nds) => nds.filter((node) => node.id !== nodeId));
      setEdges((eds) => eds.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
      setSelectedNode(null);
      return;
    }
    setNodes((nds) => 
      nds.map((node) => {
        if (node.id === nodeId) {
          return { ...node, data: newData };
        }
        return node;
      })
    );
  }, [setNodes, setEdges]);

  // Save Workflow
  const handleSave = async () => {
    await updateWorkflow.mutateAsync({
      id,
      name: workflowName,
      nodes,
      edges,
    });
  };

  // Execute Workflow
  const handleExecute = async () => {
    await handleSave(); // Auto-save before run
    await executeWorkflow.mutateAsync(id);
    setShowLogs(true);
  };

  // AI Generated Content
  const handleAIGenerated = (data: any) => {
    setNodes(data.nodes);
    setEdges(data.edges);
    if (data.name) setWorkflowName(data.name);
  };

  if (isLoading) {
    return (
      <div className="h-screen w-full flex items-center justify-center bg-background">
        <Loader2 className="w-8 h-8 text-primary animate-spin" />
      </div>
    );
  }

  if (!workflow) {
    return <div className="p-8 text-center text-red-500">Workflow not found</div>;
  }

  return (
    <div className="h-screen w-full flex flex-col bg-background overflow-hidden">
      {/* Top Bar */}
      <header className="h-14 border-b border-border bg-card flex items-center justify-between px-4 z-20">
        <div className="flex items-center gap-4">
          <Link href="/">
            <Button variant="ghost" size="icon" className="h-8 w-8">
              <ChevronLeft className="w-5 h-5" />
            </Button>
          </Link>
          <div className="h-6 w-px bg-border" />
          <Input 
            value={workflowName} 
            onChange={(e) => setWorkflowName(e.target.value)}
            className="h-8 w-64 bg-transparent border-transparent hover:border-border focus:border-primary transition-all font-semibold text-lg px-2"
          />
          <div className="h-6 w-px bg-border" />
          <div className="flex items-center gap-2">
            <span className="text-[10px] uppercase font-bold text-muted-foreground whitespace-nowrap">Swagger URL</span>
            <Input 
              placeholder="https://api.example.com/swagger.json"
              value={workflow.swaggerUrl || ""}
              onChange={(e) => {
                updateWorkflow.mutate({
                  id,
                  swaggerUrl: e.target.value
                });
              }}
              className="h-7 w-64 text-xs bg-muted/30"
            />
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={() => setShowAIModal(true)}
            className="text-purple-400 border-purple-400/20 hover:bg-purple-400/10"
          >
            <Sparkles className="w-4 h-4 mr-2" />
            AI Generate
          </Button>

          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleSave}
            disabled={updateWorkflow.isPending}
          >
            <Save className="w-4 h-4 mr-2" />
            {updateWorkflow.isPending ? "Saving..." : "Save"}
          </Button>

          <Button 
            size="sm" 
            onClick={() => {
              if (workflow?.swaggerUrl) {
                const testPrompt = `Analyze the Swagger at ${workflow.swaggerUrl} and create a comprehensive automated test workflow that checks every single API endpoint found. Each test should verify the status code is 200.`;
                setShowAIModal(true);
                // We'll need to pass this prompt to the modal somehow or handle it here
              }
            }}
            className="bg-emerald-600 hover:bg-emerald-700 text-white"
          >
            <Zap className="w-4 h-4 mr-2" />
            Auto-Test APIs
          </Button>

          <Button 
            size="sm" 
            onClick={handleExecute}
            disabled={executeWorkflow.isPending}
            className="bg-primary text-primary-foreground"
          >
            {executeWorkflow.isPending ? (
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            ) : (
              <Play className="w-4 h-4 mr-2 fill-current" />
            )}
            Execute
          </Button>
          
          <Sheet open={showLogs} onOpenChange={setShowLogs}>
            <SheetTrigger asChild>
              <Button variant="ghost" size="icon" className={showLogs ? "bg-accent" : ""}>
                <LayoutList className="w-5 h-5" />
              </Button>
            </SheetTrigger>
            <SheetContent side="bottom" className="h-[400px] p-0">
               <div className="h-full flex flex-col">
                 <div className="p-4 border-b border-border bg-muted/20">
                   <h3 className="font-semibold">Execution History</h3>
                 </div>
                 <div className="flex-1 overflow-hidden">
                   <ExecutionsList workflowId={id} />
                 </div>
               </div>
            </SheetContent>
          </Sheet>
        </div>
      </header>

      {/* Editor Layout */}
      <div className="flex-1 flex overflow-hidden">
        <Sidebar />

        <div className="flex-1 relative h-full" ref={reactFlowWrapper}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onInit={setReactFlowInstance}
            onDrop={onDrop}
            onDragOver={onDragOver}
            onNodeClick={onNodeClick}
            onPaneClick={onPaneClick}
            nodeTypes={nodeTypes}
            fitView
            snapToGrid
            className="bg-background"
          >
            <Background color="#333" gap={20} size={1} />
            <Controls className="bg-card border-border text-foreground" />
            
            {/* Properties Panel (conditionally rendered over canvas or pushed) */}
            {selectedNode && (
              <Panel position="top-right" className="m-0 h-full max-h-screen">
                <div />
              </Panel>
            )}
          </ReactFlow>

          {/* Properties Panel Overlay */}
          <PropertiesPanel 
            selectedNode={selectedNode}
            onChange={onNodeUpdate}
            onClose={() => setSelectedNode(null)}
          />
        </div>
      </div>

      <AIGenerateModal 
        open={showAIModal} 
        onOpenChange={setShowAIModal}
        onGenerated={handleAIGenerated}
      />
    </div>
  );
}

export default function WorkflowEditor() {
  const [match, params] = useRoute("/workflow/:id");
  const id = params?.id ? parseInt(params.id) : 0;

  if (!match || !id) return <div>Invalid Workflow ID</div>;

  return (
    <ReactFlowProvider>
      <EditorContent id={id} />
    </ReactFlowProvider>
  );
}
