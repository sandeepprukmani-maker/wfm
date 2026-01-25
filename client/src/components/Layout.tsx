import { Sidebar } from "./Sidebar";
import { useAuth } from "@/hooks/use-auth";
import { Button } from "@/components/ui/button";
import { Loader2 } from "lucide-react";

export function Layout({ children }: { children: React.ReactNode }) {
  // const { isAuthenticated, isLoading } = useAuth();
  const isAuthenticated = true;
  const isLoading = false;

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background text-foreground">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4 relative overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-primary/20 via-background to-background z-0" />
        <div className="relative z-10 w-full max-w-md space-y-8 text-center">
          <div>
            <h1 className="text-4xl font-bold tracking-tight mb-2">Orchestrate</h1>
            <p className="text-muted-foreground">Please sign in to access the dashboard.</p>
          </div>
          <Button 
            size="lg" 
            className="w-full bg-primary hover:bg-primary/90 shadow-lg shadow-primary/25"
            onClick={() => window.location.href = "/api/login"}
          >
            Sign In with Replit
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background text-foreground flex">
      <Sidebar />
      <main className="flex-1 ml-64 p-8 overflow-y-auto max-h-screen">
        <div className="max-w-7xl mx-auto h-full">
          {children}
        </div>
      </main>
    </div>
  );
}
