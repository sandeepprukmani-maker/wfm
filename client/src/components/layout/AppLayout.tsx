import { ReactNode } from "react";
import { Sidebar } from "./Sidebar";
import { useAuth } from "@/hooks/use-auth";
import { Loader2 } from "lucide-react";

export function AppLayout({ children }: { children: ReactNode }) {
  const { isLoading, isAuthenticated } = useAuth();

  if (isLoading) {
    return (
      <div className="flex h-screen w-full items-center justify-center bg-background">
        <div className="flex flex-col items-center gap-4">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <p className="text-sm text-muted-foreground animate-pulse">Initializing app...</p>
        </div>
      </div>
    );
  }

  // If not authenticated, the pages themselves should handle redirect
  // or show a public landing page. But for the core app layout:
  if (!isAuthenticated) {
    return null; 
  }

  return (
    <div className="flex h-screen bg-background text-foreground overflow-hidden">
      <Sidebar />
      <main className="flex-1 overflow-auto relative">
        <div className="mx-auto max-w-7xl p-8 min-h-full">
          {children}
        </div>
      </main>
    </div>
  );
}
