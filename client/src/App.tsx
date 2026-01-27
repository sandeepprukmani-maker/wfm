import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/not-found";
import { Layout } from "@/components/Layout";
import { ThemeProvider } from "@/hooks/use-theme";

// Pages
import ChatDashboard from "@/pages/Dashboard";
import ReplitDashboard from "@/pages/ReplitDashboard";
import WorkflowList from "@/pages/WorkflowList";
import WorkflowEditor from "@/pages/WorkflowEditor";
import Credentials from "@/pages/Credentials";
import Executions from "@/pages/Executions";

function Router() {
  return (
    <Layout>
      <Switch>
        <Route path="/" component={ChatDashboard} />
        <Route path="/dashboard" component={ReplitDashboard} />
        <Route path="/workflows" component={WorkflowList} />
        <Route path="/workflows/new" component={() => {
           // Redirect logic handled in list component for simplicity or just render list
           // Ideally new could be a modal on list page. We implemented it as a modal.
           // So just routing /workflows/new to list page with auto-open modal would be complex.
           // For this MVP, let's keep it simple: /workflows handles creation via modal.
           return <WorkflowList />
        }} />
        <Route path="/workflows/:id" component={WorkflowEditor} />
        <Route path="/credentials" component={Credentials} />
        <Route path="/executions" component={Executions} />
        <Route component={NotFound} />
      </Switch>
    </Layout>
  );
}

import { SidebarProvider } from "@/components/ui/sidebar"

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider>
          <SidebarProvider defaultOpen={false}>
            <Toaster />
            <Router />
          </SidebarProvider>
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}

export default App;
