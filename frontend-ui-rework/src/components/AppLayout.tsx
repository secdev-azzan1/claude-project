import { ReactNode } from "react";
import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "./AppSidebar";

interface Props {
  title: string;
  description?: string;
  actions?: ReactNode;
  children: ReactNode;
}

/**
 * The page frame: a translucent sticky header over the full available content
 * width.
 *
 * The header is sticky and blurred rather than scrolling away with the content.
 * Two reasons: page actions (Save, Deploy, Refresh) stay reachable from the
 * bottom of a long form, and a toolbar that samples the content moving under it
 * is most of what makes a window feel like an application rather than a
 * document.
 */
export function AppLayout({ title, description, actions, children }: Props) {
  return (
    <SidebarProvider>
      <div className="flex min-h-svh w-full bg-background">
        <AppSidebar />
        <div className="flex min-w-0 flex-1 flex-col">
          <header className="material-regular sticky top-0 z-30 border-b border-border/60">
            <div className="flex w-full items-center gap-3 px-4 py-3 md:px-6 lg:px-8 2xl:px-10">
              <SidebarTrigger className="-ml-1 shrink-0 md:hidden" />
              <div className="min-w-0 flex-1">
                <h1 className="truncate text-xl font-semibold tracking-tight">{title}</h1>
                {description && (
                  <p className="truncate text-sm leading-snug text-muted-foreground">{description}</p>
                )}
              </div>
              {actions && <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">{actions}</div>}
            </div>
          </header>
          <main className="w-full flex-1 px-4 py-6 md:px-6 md:py-8 lg:px-8 2xl:px-10">{children}</main>
        </div>
      </div>
    </SidebarProvider>
  );
}
