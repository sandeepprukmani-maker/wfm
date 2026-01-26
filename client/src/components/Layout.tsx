import { AppSidebar } from "./Sidebar";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { useAuth } from "@/hooks/use-auth";
import { Button } from "@/components/ui/button";
import { Loader2, Workflow } from "lucide-react";

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
    <div className="min-h-screen bg-background text-foreground flex w-full">
      <AppSidebar />
      <div className="flex flex-col flex-1 min-w-0">
        <header className="flex items-center h-14 px-4 border-b bg-background sticky top-0 z-30">
          <div className="flex items-center gap-4 w-full relative">
            <SidebarTrigger className="h-8 w-8 text-muted-foreground hover:text-foreground" />
            
            <div className="absolute left-1/2 -translate-x-1/2 flex items-center gap-2">
              <div className="w-6 h-6 rounded-md bg-primary flex items-center justify-center shrink-0 shadow-sm">
                <Workflow className="text-primary-foreground w-3.5 h-3.5" />
              </div>
              <span className="font-semibold text-sm tracking-tight">Orchestrator</span>
            </div>
            
            <div className="flex-1" />
          </div>
        </header>
        <main className="flex-1 overflow-y-auto">
          <div className="max-w-7xl mx-auto h-full px-4 py-6 md:px-8 md:py-8">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
}
