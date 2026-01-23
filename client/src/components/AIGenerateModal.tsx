import { useState } from "react";
import { Sparkles } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useGenerateWorkflow } from "@/hooks/use-workflows";

interface AIGenerateModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onGenerated: (data: any) => void;
}

export default function AIGenerateModal({ open, onOpenChange, onGenerated }: AIGenerateModalProps) {
  const [prompt, setPrompt] = useState("");
  const generateMutation = useGenerateWorkflow();

  const handleGenerate = async () => {
    if (!prompt.trim()) return;
    
    try {
      const result = await generateMutation.mutateAsync({ prompt });
      onGenerated(result);
      onOpenChange(false);
      setPrompt("");
    } catch (error) {
      console.error(error);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="w-5 h-5 text-primary" />
            Generate Workflow with AI
          </DialogTitle>
          <DialogDescription>
            Describe what you want your workflow to do, and our AI will build the nodes and connections for you.
          </DialogDescription>
        </DialogHeader>

        <div className="py-4">
          <Textarea
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            placeholder="e.g., Check the status of my server every 5 minutes and send a Slack notification if it's down."
            className="min-h-[120px] text-base"
          />
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button 
            onClick={handleGenerate} 
            disabled={generateMutation.isPending || !prompt.trim()}
            className="gap-2"
          >
            {generateMutation.isPending ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Generating...
              </>
            ) : (
              <>
                <Sparkles className="w-4 h-4" />
                Generate
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
