import { Link, useLocation } from "wouter";
import { 
  LayoutDashboard, 
  Workflow, 
  Key, 
  Activity, 
  Settings,
  LogOut
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuth } from "@/hooks/use-auth";

export function Sidebar() {
  const [location] = useLocation();
  // const { logout, user } = useAuth();
  const logout = () => window.location.href = "/api/logout";
  const user = { firstName: "Admin", email: "admin@workflow.local", profileImageUrl: null };

  const navItems = [
    { icon: LayoutDashboard, label: "Overview", href: "/" },
    { icon: Workflow, label: "Workflows", href: "/workflows" },
    { icon: Activity, label: "Executions", href: "/executions" },
    { icon: Key, label: "Credentials", href: "/credentials" },
  ];

  return (
    <aside className="w-64 border-r border-border bg-card flex flex-col h-screen fixed left-0 top-0 z-10">
      <div className="p-6 border-b border-border flex items-center gap-3">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-primary to-purple-600 flex items-center justify-center">
          <Workflow className="text-white w-5 h-5" />
        </div>
        <h1 className="font-bold text-xl tracking-tight">Orchestrate</h1>
      </div>

      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => {
          const isActive = location === item.href || (item.href !== '/' && location.startsWith(item.href));
          return (
            <Link key={item.href} href={item.href} className={cn(
              "flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium transition-all duration-200 group",
              isActive 
                ? "bg-primary/10 text-primary shadow-sm" 
                : "text-muted-foreground hover:bg-muted hover:text-foreground"
            )}>
              <item.icon className={cn(
                "w-5 h-5 transition-colors",
                isActive ? "text-primary" : "text-muted-foreground group-hover:text-foreground"
              )} />
              {item.label}
            </Link>
          );
        })}
      </nav>

      <div className="p-4 border-t border-border">
        <div className="bg-muted/30 rounded-xl p-4 flex items-center gap-3 mb-2">
          {user?.profileImageUrl ? (
            <img src={user.profileImageUrl} alt="User" className="w-10 h-10 rounded-full bg-background" />
          ) : (
            <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center">
               <span className="text-primary font-bold">{user?.email?.[0]?.toUpperCase()}</span>
            </div>
          )}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium truncate">{user?.firstName || 'User'}</p>
            <p className="text-xs text-muted-foreground truncate">{user?.email}</p>
          </div>
        </div>
        <button 
          onClick={() => logout()}
          className="w-full flex items-center gap-2 px-4 py-2 text-sm text-muted-foreground hover:text-destructive hover:bg-destructive/10 rounded-lg transition-colors"
        >
          <LogOut className="w-4 h-4" />
          Sign Out
        </button>
      </div>
    </aside>
  );
}
