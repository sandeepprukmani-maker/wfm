import { Link, useLocation } from "wouter";
import { 
  LayoutDashboard, 
  Workflow, 
  Key, 
  Activity, 
  Settings,
  LogOut,
  History,
  Layout,
  Sun,
  Moon
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarTrigger,
  useSidebar
} from "@/components/ui/sidebar";
import { useTheme } from "@/hooks/use-theme";

export function AppSidebar() {
  const [location] = useLocation();
  const { theme, setTheme } = useTheme();
  const logout = () => window.location.href = "/api/logout";
  const user = { firstName: "Admin", email: "admin@workflow.local", profileImageUrl: null };
  const { state } = useSidebar();

  const navItems = [
    { icon: LayoutDashboard, label: "Chat", href: "/" },
    { icon: Layout, label: "Dashboard", href: "/dashboard" },
    { icon: Workflow, label: "Workflows", href: "/workflows" },
    { icon: History, label: "Executions", href: "/executions" },
    { icon: Key, label: "Credentials", href: "/credentials" },
  ];

  return (
    <Sidebar collapsible="icon" className="border-r border-border bg-background shadow-none">
      <SidebarHeader className="h-14 border-b border-border flex items-center justify-between px-4 bg-background">
        <div className="flex items-center gap-3 overflow-hidden">
          <div className="w-7 h-7 rounded-md bg-primary flex items-center justify-center shrink-0">
            <Workflow className="text-primary-foreground w-4 h-4" />
          </div>
          {state === "expanded" && <span className="font-semibold text-sm tracking-tight truncate">Orchestrate</span>}
        </div>
      </SidebarHeader>

      <SidebarContent className="px-2 py-4 bg-background">
        <SidebarMenu>
          {navItems.map((item) => (
            <SidebarMenuItem key={item.href}>
              <SidebarMenuButton 
                asChild 
                isActive={location === item.href}
                tooltip={item.label}
                className="h-9 px-3"
              >
                <Link href={item.href}>
                  <item.icon className="w-4 h-4" />
                  <span>{item.label}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarContent>

      <SidebarFooter className="p-2 border-t border-border bg-background">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton 
              onClick={() => setTheme(theme === "dark" ? "light" : "dark")} 
              tooltip={theme === "dark" ? "Switch to Light Mode" : "Switch to Dark Mode"}
              className="h-9 px-3"
            >
              {theme === "dark" ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
              <span>{theme === "dark" ? "Light Mode" : "Dark Mode"}</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <div className={cn(
              "flex items-center gap-3 p-2 rounded-lg transition-all duration-200",
              state === "expanded" ? "hover:bg-muted/50" : ""
            )}>
              <div className="w-7 h-7 rounded-full bg-muted flex items-center justify-center shrink-0 border border-border">
                <span className="text-muted-foreground font-medium text-[10px]">{user?.email?.[0]?.toUpperCase()}</span>
              </div>
              {state === "expanded" && (
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium truncate">Admin</p>
                  <p className="text-[10px] text-muted-foreground truncate">Free Plan</p>
                </div>
              )}
            </div>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton onClick={() => logout()} tooltip="Sign Out" className="h-8 text-xs">
              <LogOut className="w-3.5 h-3.5" />
              <span>Sign Out</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  );
}
