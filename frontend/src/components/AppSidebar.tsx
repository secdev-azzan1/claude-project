import { useEffect, useState } from "react";
import {
  LayoutDashboard,
  FileCode2,
  PlayCircle,
  ScrollText,
  Database,
  Cable,
  Globe,
  Server,
  RefreshCcw,
} from "lucide-react";
import { NavLink } from "@/components/NavLink";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarFooter,
  useSidebar,
} from "@/components/ui/sidebar";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { resetDemoData } from "@/prototype/api";
import { consumeMigrationNotice } from "@/prototype/store";
import { toast } from "sonner";

const mainItems = [
  { title: "Dashboard", url: "/", icon: LayoutDashboard },
  { title: "Flows", url: "/flows", icon: PlayCircle },
  { title: "Schemas", url: "/schemas", icon: FileCode2 },
  { title: "Application Services", url: "/application-services", icon: Server },
  { title: "Audit Log", url: "/audit", icon: ScrollText },
];

const systemItems = [
  { title: "Platform Connections", url: "/connections", icon: Cable },
  { title: "APISIX Gateway", url: "/apisix", icon: Globe },
];

const MIGRATION_TOAST_ID = "seed-migration-notice";

export function AppSidebar() {
  const { state, isMobile } = useSidebar();
  const [notice, setNotice] = useState<string | null>(null);

  // The store stamps this exactly once, when a stale saved dataset was archived
  // and reseeded. Consuming it clears it, so it survives at most one mount.
  useEffect(() => {
    const pending = consumeMigrationNotice();
    if (pending) setNotice(pending);
  }, []);

  // The footer line below cannot be seen from the icon rail (or on mobile,
  // where the sidebar is an off-canvas sheet). In those states the notice is
  // raised as a persistent toast instead, so it is never simply lost.
  const footerHidden = state === "collapsed" || isMobile;
  useEffect(() => {
    if (!notice || !footerHidden) return;
    toast.info("Demo data was reset by an update", {
      id: MIGRATION_TOAST_ID,
      description: notice,
      duration: Infinity,
      action: { label: "Dismiss", onClick: () => toast.dismiss(MIGRATION_TOAST_ID) },
    });
  }, [notice, footerHidden]);

  const dismissNotice = () => {
    setNotice(null);
    toast.dismiss(MIGRATION_TOAST_ID);
  };

  const handleReset = () => {
    resetDemoData();
    toast.success("Demo data reset to the seed state");
    setTimeout(() => window.location.reload(), 400);
  };

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="border-b">
        <div className="flex items-center gap-2 px-2 py-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <Database className="h-4 w-4" />
          </div>
          <div className="flex flex-col leading-tight group-data-[collapsible=icon]:hidden">
            <span className="text-sm font-semibold">Data Mobility Platform</span>
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Adapter UI prototype
            </span>
          </div>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {mainItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild tooltip={item.title}>
                    <NavLink
                      to={item.url}
                      end={item.url === "/"}
                      className="text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                      activeClassName="bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                    >
                      <item.icon className="h-4 w-4" />
                      <span>{item.title}</span>
                    </NavLink>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>System</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {systemItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild tooltip={item.title}>
                    <NavLink
                      to={item.url}
                      className="text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                      activeClassName="bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                    >
                      <item.icon className="h-4 w-4" />
                      <span>{item.title}</span>
                    </NavLink>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="border-t">
        {notice && (
          <div className="px-2 pt-2 group-data-[collapsible=icon]:hidden">
            <div className="rounded-md border border-warning/30 bg-warning-muted px-2 py-1.5">
              <p className="text-xs font-medium leading-4">Demo data was reset by an update</p>
              <p className="mt-0.5 text-xs leading-4 text-muted-foreground">{notice}</p>
              <button
                type="button"
                className="mt-1 text-xs font-medium underline underline-offset-2 hover:text-foreground"
                onClick={dismissNotice}
              >
                Dismiss
              </button>
            </div>
          </div>
        )}
        <div className="flex flex-col gap-2 px-2 py-2 group-data-[collapsible=icon]:hidden">
          <div className="rounded-md border border-dashed bg-muted/50 px-2 py-1.5">
            <p className="text-xs leading-4 text-muted-foreground">
              Frontend-only prototype — every action is simulated and persisted in your browser.
            </p>
          </div>
          <Button variant="outline" size="sm" className="h-7 gap-1.5 text-xs" onClick={handleReset}>
            <RefreshCcw className="h-3 w-3" />
            Reset demo data
          </Button>
        </div>
        {/* Collapsed icon rail: the block above is hidden, so Reset would be
            unreachable. This icon-only twin keeps it one click away. */}
        <div className="hidden justify-center px-0 py-2 group-data-[collapsible=icon]:flex">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="icon"
                className="h-8 w-8"
                aria-label="Reset demo data"
                onClick={handleReset}
              >
                <RefreshCcw className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="right">Reset demo data</TooltipContent>
          </Tooltip>
        </div>
        <div className="flex items-center gap-2 px-2 py-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-accent text-accent-foreground text-xs font-semibold">
            A
          </div>
          <div className="flex flex-col leading-tight group-data-[collapsible=icon]:hidden">
            <span className="text-sm font-medium">admin</span>
            <span className="text-xs text-muted-foreground">Platform Admin</span>
          </div>
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
