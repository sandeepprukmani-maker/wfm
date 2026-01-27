import { useState, useEffect, useRef } from "react";
import { useWorkflows, useCreateWorkflow } from "@/hooks/use-workflows";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ArrowRight, Bot, Loader2, Send, User, Workflow as WorkflowIcon } from "lucide-react";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { Link } from "wouter";

type Message = {
  role: "user" | "assistant";
  content: string;
  workflowId?: number;
};

export default function ChatDashboard() {
  const [messages, setMessages] = useState<Message[]>([
    { role: "assistant", content: "Hello! I can help you create and manage Airflow workflows. What would you like to build today?" }
  ]);
  const [input, setInput] = useState("");
  const [isGenerating, setIsGenerating] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();
  const { mutateAsync: createWorkflow } = useCreateWorkflow();

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || isGenerating) return;

    const userMessage = input.trim();
    setInput("");
    setMessages(prev => [...prev, { role: "user", content: userMessage }]);
    setIsGenerating(true);

    try {
      const res = await apiRequest("POST", "/api/workflows/generate", { prompt: userMessage });
      const workflowData = await res.json();

      // Create the workflow
      const newWorkflow = await createWorkflow({
        name: `Generated: ${userMessage.slice(0, 20)}...`,
        description: `Generated from prompt: ${userMessage}`,
        nodes: workflowData.nodes,
        edges: workflowData.edges
      });

      setMessages(prev => [...prev, { 
        role: "assistant", 
        content: "I've generated the workflow for you. You can now view and edit it:",
        workflowId: newWorkflow.id
      }]);

      toast({
        title: "Workflow Generated",
        description: "Your new workflow has been created.",
      });

    } catch (error) {
      console.error("Failed to generate workflow:", error);
      setMessages(prev => [...prev, { 
        role: "assistant", 
        content: "I'm sorry, I encountered an error while generating your workflow. Please try again." 
      }]);
      toast({
        variant: "destructive",
        title: "Generation Failed",
        description: "Could not create workflow. Please check your prompt.",
      });
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-140px)] max-w-4xl mx-auto py-4">
      <div className="flex-1 overflow-hidden flex flex-col bg-background border-none">
        <ScrollArea className="flex-1 px-1" ref={scrollRef}>
          <div className="space-y-8 py-4">
            {messages.map((msg, i) => (
              <div key={i} className={`flex gap-4 ${msg.role === "user" ? "flex-row-reverse" : ""}`}>
                {msg.role === "user" && (
                  <div className="w-8 h-8 rounded-full flex items-center justify-center shrink-0 border border-border bg-primary text-primary-foreground">
                    <User className="w-4 h-4" />
                  </div>
                )}
                <div className={`flex-1 flex flex-col ${msg.role === "user" ? "items-end" : "items-start"}`}>
                  <div className={`max-w-[85%] text-sm leading-relaxed p-4 rounded-2xl ${
                    msg.role === "user"
                      ? "bg-primary text-primary-foreground rounded-tr-none shadow-sm"
                      : "bg-muted text-foreground border border-border rounded-tl-none shadow-sm"
                  }`}>
                    {msg.content}
                    {msg.workflowId && (
                      <div className="mt-4">
                        <Link href={`/workflows/${msg.workflowId}`}>
                          <Button variant="secondary" size="sm" className="gap-2 bg-background hover:bg-background/90 text-foreground border border-border">
                            Open Workflow Editor <ArrowRight className="w-3 h-3" />
                          </Button>
                        </Link>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            ))}
            {isGenerating && (
              <div className="flex gap-4">
                <div className="w-8 h-8 rounded-full bg-muted text-muted-foreground flex items-center justify-center shrink-0 border border-border">
                  <Bot className="w-4 h-4" />
                </div>
                <div className="bg-muted/50 border border-border p-4 rounded-2xl rounded-tl-none flex items-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span className="text-sm">Generating workflow...</span>
                </div>
              </div>
            )}
          </div>
        </ScrollArea>

        <div className="p-4 bg-background">
          <form 
            onSubmit={(e) => { e.preventDefault(); handleSend(); }}
            className="relative"
          >
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Message Orchestrator..."
              className="w-full pl-4 pr-12 py-6 bg-muted/50 border-border focus-visible:ring-1 rounded-2xl"
              disabled={isGenerating}
              data-testid="input-chat"
            />
            <Button 
              type="submit" 
              size="icon" 
              variant="ghost"
              className="absolute right-2 top-1/2 -translate-y-1/2 h-8 w-8 hover:bg-transparent"
              disabled={isGenerating || !input.trim()}
              data-testid="button-send"
            >
              <Send className={`w-4 h-4 ${input.trim() ? "text-primary" : "text-muted-foreground"}`} />
            </Button>
          </form>
          <p className="text-[10px] text-center text-muted-foreground mt-2">
            AI can make mistakes. Check important info.
          </p>
        </div>
      </div>
    </div>
  );
}
