import {
  LayoutDashboard,
  FileCode2,
  PlayCircle,
  ScrollText,
  Database,
  Cable,
  Globe,
  Server,
} from "lucide-react";
import { NavLink } from "@/components/NavLink";
import { ThemeToggle } from "@/components/ThemeToggle";
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
} from "@/components/ui/sidebar";

const mainItems = [
  { title: "Dashboard", url: "/", icon: LayoutDashboard },
  { title: "Flows", url: "/flows", icon: PlayCircle },
  { title: "Schemas", url: "/schemas", icon: FileCode2 },
  { title: "Application Services", url: "/application-services", icon: Server },
  { title: "Audit Log", url: "/audit", icon: ScrollText },
];

const systemItems = [
  { title: "Platform Connections", url: "/connections", icon: Cable },
  { title: "HTTP Proxies", url: "/apisix", icon: Globe },
];

export function AppSidebar() {
  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="px-2 py-3">
        <div className="flex items-center gap-2.5 px-1">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground shadow-sm">
            <Database className="h-[18px] w-[18px]" />
          </div>
          <div className="flex min-w-0 flex-col leading-tight group-data-[collapsible=icon]:hidden">
            <span className="truncate text-sm font-semibold tracking-tight">Data Mobility</span>
            <span className="truncate text-2xs text-muted-foreground">Adapter platform</span>
          </div>
        </div>
      </SidebarHeader>

      <SidebarContent className="px-1.5">
        <SidebarGroup>
          <SidebarGroupLabel>Workspace</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {mainItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  {/* NavLink resolves `active` itself, so the active styling is
                      handed down as activeClassName rather than through
                      SidebarMenuButton's isActive prop. */}
                  <SidebarMenuButton asChild tooltip={item.title}>
                    <NavLink
                      to={item.url}
                      end={item.url === "/"}
                      activeClassName="bg-sidebar-accent text-sidebar-accent-foreground font-medium shadow-sm [&>svg]:text-primary"
                    >
                      <item.icon />
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
                      activeClassName="bg-sidebar-accent text-sidebar-accent-foreground font-medium shadow-sm [&>svg]:text-primary"
                    >
                      <item.icon />
                      <span>{item.title}</span>
                    </NavLink>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="gap-3 px-2 pb-3">
        {/* The dashed "Connected to the live backend." box that used to sit here
            was a permanent sentence stating the normal case — it only ever said
            anything when nothing was wrong. */}
        <div className="group-data-[collapsible=icon]:hidden">
          <ThemeToggle />
        </div>
        <div className="flex items-center gap-2.5 px-1">
          <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-primary-muted text-2xs font-semibold text-primary">
            A
          </div>
          <div className="flex min-w-0 flex-col leading-tight group-data-[collapsible=icon]:hidden">
            <span className="truncate text-sm font-medium">admin</span>
            <span className="truncate text-2xs text-muted-foreground">Platform Admin</span>
          </div>
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
