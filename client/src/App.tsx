import { Switch, Route, Redirect } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useAuth } from "@/hooks/use-auth";
import { useEffect } from "react";
import { useToast } from "@/hooks/use-toast";
import NotFound from "@/pages/not-found";
import Home from "@/pages/Home";
import Dashboard from "@/pages/Dashboard";
import WorkflowBuilder from "@/pages/WorkflowBuilder";
import ExecutionsList from "@/pages/ExecutionsList";
import ExecutionDetail from "@/pages/ExecutionDetail";

// Protected Route Wrapper
function ProtectedRoute({ component: Component, ...rest }: any) {
  const { user, isLoading } = useAuth();
  
  if (isLoading) return null; // Or a loading spinner

  if (!user) {
    return <Redirect to="/" />;
  }

  return <Component {...rest} />;
}

// Auth Redirect Handler
function AuthHandler() {
  const { isAuthenticated, isLoading } = useAuth();
  const { toast } = useToast();

  useEffect(() => {
    if (!isLoading && !isAuthenticated && window.location.pathname !== "/") {
      // Only toast on protected routes if we are forcefully redirecting?
      // Actually, ProtectedRoute handles the redirect. This is just for global 401 monitoring if needed.
    }
  }, [isAuthenticated, isLoading, toast]);

  return null;
}

function Router() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      
      {/* Protected Routes */}
      <Route path="/dashboard">
        {() => <ProtectedRoute component={Dashboard} />}
      </Route>
      
      <Route path="/workflows">
        {() => <ProtectedRoute component={Dashboard} />} 
      </Route>

      <Route path="/workflows/:id">
        {() => <ProtectedRoute component={WorkflowBuilder} />}
      </Route>

      <Route path="/executions">
        {() => <ProtectedRoute component={ExecutionsList} />}
      </Route>

      <Route path="/executions/:id">
        {() => <ProtectedRoute component={ExecutionDetail} />}
      </Route>

      {/* Fallback to 404 */}
      <Route component={NotFound} />
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <AuthHandler />
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
