import { cn } from "@/lib/utils";

// A soft pulse rather than the stock full-opacity one — a loading placeholder
// that flashes as hard as real content is more distracting than the wait.
function Skeleton({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("animate-pulse rounded-lg bg-muted/70", className)} {...props} />;
}

export { Skeleton };
