import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { useAuth } from "@/hooks/use-auth";
import { ArrowRight, Workflow, Zap, Lock, BarChart } from "lucide-react";

export default function Home() {
  const { isAuthenticated } = useAuth();

  if (isAuthenticated) {
    // If authenticated, redirect logic should normally be handled by the router
    // but here we can just show a button to go to dashboard
    window.location.href = "/dashboard";
    return null;
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Navbar */}
      <nav className="border-b bg-card/50 backdrop-blur-xl fixed w-full z-50">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="h-8 w-8 bg-primary rounded-lg flex items-center justify-center">
              <Workflow className="h-5 w-5 text-primary-foreground" />
            </div>
            <span className="font-display text-xl font-bold">FlowOrchestra</span>
          </div>
          <div className="flex items-center gap-4">
            <a href="/api/login">
              <Button>Log In</Button>
            </a>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <div className="pt-32 pb-20 px-6">
        <div className="max-w-7xl mx-auto grid lg:grid-cols-2 gap-12 items-center">
          <div>
            <div className="inline-flex items-center rounded-full border px-3 py-1 text-sm font-medium bg-accent/50 text-accent-foreground mb-6">
              <span className="flex h-2 w-2 rounded-full bg-primary mr-2 animate-pulse"></span>
              v2.0 Now Available
            </div>
            <h1 className="text-5xl md:text-7xl font-display font-bold leading-tight mb-6">
              Orchestrate your <span className="text-primary">data workflows</span> visually.
            </h1>
            <p className="text-xl text-muted-foreground mb-8 leading-relaxed">
              Build, schedule, and monitor complex data pipelines with our drag-and-drop editor. 
              Combine SQL, Python, and APIs into powerful automated workflows.
            </p>
            <div className="flex flex-col sm:flex-row gap-4">
              <a href="/api/login">
                <Button size="lg" className="h-12 px-8 text-base shadow-lg shadow-primary/20 hover:shadow-primary/30 hover:-translate-y-0.5 transition-all">
                  Start Building Free <ArrowRight className="ml-2 h-5 w-5" />
                </Button>
              </a>
              <Button size="lg" variant="outline" className="h-12 px-8 text-base">
                View Documentation
              </Button>
            </div>
          </div>
          
          <div className="relative">
            <div className="absolute -inset-1 bg-gradient-to-r from-primary to-purple-600 rounded-2xl blur opacity-30 animate-pulse"></div>
            <div className="relative rounded-2xl border bg-card/50 backdrop-blur-sm shadow-2xl overflow-hidden">
               {/* Abstract UI representation */}
               <div className="border-b bg-muted/50 p-4 flex gap-2">
                 <div className="h-3 w-3 rounded-full bg-red-400"></div>
                 <div className="h-3 w-3 rounded-full bg-yellow-400"></div>
                 <div className="h-3 w-3 rounded-full bg-green-400"></div>
               </div>
               <div className="p-8 aspect-video bg-grid-slate-200/50 [mask-image:linear-gradient(0deg,white,rgba(255,255,255,0.6))] dark:bg-grid-slate-800/50 flex items-center justify-center">
                  <div className="flex gap-8 items-center">
                    <div className="h-16 w-16 rounded-xl bg-blue-500/20 border-2 border-blue-500 flex items-center justify-center shadow-lg shadow-blue-500/20">
                      <Database className="h-8 w-8 text-blue-500" />
                    </div>
                    <div className="h-0.5 w-16 bg-border relative">
                      <div className="absolute right-0 -top-1 h-2 w-2 border-t-2 border-r-2 border-border rotate-45"></div>
                    </div>
                    <div className="h-16 w-16 rounded-xl bg-yellow-500/20 border-2 border-yellow-500 flex items-center justify-center shadow-lg shadow-yellow-500/20">
                      <Zap className="h-8 w-8 text-yellow-500" />
                    </div>
                    <div className="h-0.5 w-16 bg-border relative">
                      <div className="absolute right-0 -top-1 h-2 w-2 border-t-2 border-r-2 border-border rotate-45"></div>
                    </div>
                    <div className="h-16 w-16 rounded-xl bg-green-500/20 border-2 border-green-500 flex items-center justify-center shadow-lg shadow-green-500/20">
                      <BarChart className="h-8 w-8 text-green-500" />
                    </div>
                  </div>
               </div>
            </div>
          </div>
        </div>
      </div>

      {/* Features Grid */}
      <div className="bg-muted/30 py-24 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="grid md:grid-cols-3 gap-8">
            <div className="p-8 rounded-2xl bg-card border shadow-sm hover:shadow-md transition-all">
              <div className="h-12 w-12 rounded-xl bg-blue-500/10 flex items-center justify-center mb-6">
                <Workflow className="h-6 w-6 text-blue-500" />
              </div>
              <h3 className="text-xl font-bold font-display mb-3">Visual Builder</h3>
              <p className="text-muted-foreground">
                Drag and drop nodes to create complex logic. No coding required for standard operations.
              </p>
            </div>
            <div className="p-8 rounded-2xl bg-card border shadow-sm hover:shadow-md transition-all">
              <div className="h-12 w-12 rounded-xl bg-purple-500/10 flex items-center justify-center mb-6">
                <Zap className="h-6 w-6 text-purple-500" />
              </div>
              <h3 className="text-xl font-bold font-display mb-3">Instant Execution</h3>
              <p className="text-muted-foreground">
                Run workflows in real-time with millisecond latency. Watch data flow through your pipeline.
              </p>
            </div>
            <div className="p-8 rounded-2xl bg-card border shadow-sm hover:shadow-md transition-all">
              <div className="h-12 w-12 rounded-xl bg-green-500/10 flex items-center justify-center mb-6">
                <Lock className="h-6 w-6 text-green-500" />
              </div>
              <h3 className="text-xl font-bold font-display mb-3">Enterprise Secure</h3>
              <p className="text-muted-foreground">
                Role-based access control, audit logs, and encrypted secrets management built-in.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// Database Icon
function Database(props: any) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  )
}
