// A three-way theme control. The `.dark` palette was fully authored in
// index.css but unreachable — nothing ever mounted a provider or offered a way
// to switch. System is the default so the app follows the OS the way a native
// one would.

import { useEffect, useState } from "react";
import { useTheme } from "next-themes";
import { Monitor, Moon, Sun } from "lucide-react";

import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

const OPTIONS = [
  { value: "light", label: "Light", icon: Sun },
  { value: "dark", label: "Dark", icon: Moon },
  { value: "system", label: "System", icon: Monitor },
] as const;

export function ThemeToggle({ className }: { className?: string }) {
  const { theme, setTheme } = useTheme();
  // next-themes resolves nothing on the server/first paint, so rendering the
  // real state straight away would flash the wrong segment as selected.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  return (
    <ToggleGroup
      type="single"
      value={mounted ? (theme ?? "system") : undefined}
      onValueChange={(v) => v && setTheme(v)}
      aria-label="Colour theme"
      className={cn("w-full", className)}
    >
      {OPTIONS.map((opt) => (
        <Tooltip key={opt.value}>
          <TooltipTrigger asChild>
            <ToggleGroupItem value={opt.value} size="sm" className="flex-1" aria-label={opt.label}>
              <opt.icon className="h-3.5 w-3.5" />
            </ToggleGroupItem>
          </TooltipTrigger>
          <TooltipContent side="top">{opt.label}</TooltipContent>
        </Tooltip>
      ))}
    </ToggleGroup>
  );
}
