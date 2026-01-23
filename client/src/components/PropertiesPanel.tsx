import { useEffect, useState } from "react";
import { Node } from "reactflow";
import { X, Save } from "lucide-react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";

interface PropertiesPanelProps {
  selectedNode: Node | null;
  onChange: (nodeId: string, data: any) => void;
  onClose: () => void;
}

export default function PropertiesPanel({ selectedNode, onChange, onClose }: PropertiesPanelProps) {
  const [formData, setFormData] = useState<any>({});

  useEffect(() => {
    if (selectedNode) {
      setFormData(selectedNode.data);
    }
  }, [selectedNode]);

  if (!selectedNode) return null;

  const handleChange = (key: string, value: any) => {
    const newData = { ...formData, [key]: value };
    setFormData(newData);
    onChange(selectedNode.id, newData);
  };

  return (
    <div className="w-80 border-l border-border bg-card shadow-2xl absolute right-0 top-0 bottom-0 z-10 flex flex-col animate-in slide-in-from-right duration-200">
      <div className="flex items-center justify-between p-4 border-b border-border">
        <div>
          <h3 className="font-semibold text-foreground">Properties</h3>
          <p className="text-xs text-muted-foreground font-mono mt-0.5">{selectedNode.type} node</p>
        </div>
        <div className="flex items-center gap-1">
          <Button 
            variant="ghost" 
            size="icon" 
            onClick={() => {
              // We'll pass a onDelete function or just handle it via onChange with a special flag
              onChange(selectedNode.id, { _delete: true });
              onClose();
            }} 
            className="h-8 w-8 text-muted-foreground hover:text-destructive transition-colors"
            title="Delete Node"
          >
            <X className="w-4 h-4" />
          </Button>
          <Button variant="ghost" size="icon" onClick={onClose} className="h-8 w-8 text-muted-foreground hover:text-foreground">
            <X className="w-4 h-4" />
          </Button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-6">
        <div className="space-y-2">
          <Label>Label</Label>
          <Input 
            value={formData.label || ""} 
            onChange={(e) => handleChange("label", e.target.value)} 
            placeholder="Node Name"
            className="bg-background"
          />
        </div>

        <Separator />

        {selectedNode.type === 'http' && (
          <>
            <div className="space-y-2">
              <Label>Method</Label>
              <Select 
                value={formData.method || "GET"} 
                onValueChange={(val) => handleChange("method", val)}
              >
                <SelectTrigger className="bg-background">
                  <SelectValue placeholder="Select method" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="GET">GET</SelectItem>
                  <SelectItem value="POST">POST</SelectItem>
                  <SelectItem value="PUT">PUT</SelectItem>
                  <SelectItem value="DELETE">DELETE</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label>URL</Label>
              <Input 
                value={formData.url || ""} 
                onChange={(e) => handleChange("url", e.target.value)} 
                placeholder="https://api.example.com/data"
                className="font-mono text-xs bg-background"
              />
            </div>

            <div className="space-y-2">
              <Label>Headers (JSON)</Label>
              <Textarea 
                value={typeof formData.headers === 'object' ? JSON.stringify(formData.headers, null, 2) : formData.headers || "{}"}
                onChange={(e) => {
                  try {
                    // Try to parse just to validate color, but store string until valid?
                    // For simplicity, just store string and let consumer handle parsing
                    handleChange("headers", e.target.value);
                  } catch(e) {}
                }}
                className="font-mono text-xs min-h-[100px] bg-background"
                placeholder='{"Content-Type": "application/json"}'
              />
            </div>
          </>
        )}

        {selectedNode.type === 'logic' && (
          <div className="space-y-2">
            <Label>Condition (JavaScript)</Label>
            <Input 
              value={formData.condition || ""} 
              onChange={(e) => handleChange("condition", e.target.value)} 
              placeholder="data.value > 10"
              className="font-mono text-xs bg-background"
            />
            <p className="text-[10px] text-muted-foreground">
              Return true to take the green path, false for red.
            </p>
          </div>
        )}

        {selectedNode.type === 'transform' && (
          <div className="space-y-2">
            <Label>Transformation Code</Label>
            <Textarea 
              value={formData.code || ""} 
              onChange={(e) => handleChange("code", e.target.value)} 
              placeholder="return { ...data, timestamp: Date.now() };"
              className="font-mono text-xs min-h-[200px] bg-background"
            />
          </div>
        )}

        {selectedNode.type === 's3' && (
          <>
            <div className="space-y-2">
              <Label>Operation</Label>
              <Select 
                value={formData.operation || "list"} 
                onValueChange={(val) => handleChange("operation", val)}
              >
                <SelectTrigger className="bg-background">
                  <SelectValue placeholder="Select operation" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="list">List Buckets</SelectItem>
                  <SelectItem value="upload">Upload File</SelectItem>
                  <SelectItem value="download">Download File</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Bucket Name</Label>
              <Input 
                value={formData.bucket || ""} 
                onChange={(e) => handleChange("bucket", e.target.value)} 
                placeholder="my-cool-bucket"
                className="bg-background"
              />
            </div>
          </>
        )}

        {selectedNode.type === 'db' && (
          <>
            <div className="space-y-2">
              <Label>DB Type</Label>
              <Select 
                value={formData.dbType || "mysql"} 
                onValueChange={(val) => handleChange("dbType", val)}
              >
                <SelectTrigger className="bg-background">
                  <SelectValue placeholder="Select DB Type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="mysql">MySQL</SelectItem>
                  <SelectItem value="mssql">SQL Server</SelectItem>
                  <SelectItem value="postgres">PostgreSQL</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>SQL Query</Label>
              <Textarea 
                value={formData.query || ""} 
                onChange={(e) => handleChange("query", e.target.value)} 
                placeholder="SELECT * FROM users LIMIT 10;"
                className="font-mono text-xs min-h-[150px] bg-background"
              />
            </div>
          </>
        )}

        {selectedNode.type === 'airflow' && (
          <>
            <div className="space-y-2">
              <Label>Operation</Label>
              <Select 
                value={formData.operation || "trigger"} 
                onValueChange={(val) => handleChange("operation", val)}
              >
                <SelectTrigger className="bg-background">
                  <SelectValue placeholder="Select operation" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="trigger">Trigger DAG</SelectItem>
                  <SelectItem value="status">Get DAG Status</SelectItem>
                  <SelectItem value="list">List Runs</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>DAG ID</Label>
              <Input 
                value={formData.dagId || ""} 
                onChange={(e) => handleChange("dagId", e.target.value)} 
                placeholder="example_dag_id"
                className="bg-background"
              />
            </div>
          </>
        )}
      </div>

      <div className="p-4 border-t border-border bg-muted/10">
        <p className="text-[10px] text-center text-muted-foreground">
          Changes are auto-applied to canvas. <br/>Save workflow to persist.
        </p>
      </div>
    </div>
  );
}
