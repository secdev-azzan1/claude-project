import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

// Badges were doing two different jobs in this app: marking a status, and
// carrying a whole sentence. Only the first is a badge. These are sized and
// weighted for a one-or-two-word label — 11px medium in a soft tint, not 12px
// semibold in a saturated fill, which is what made a row of them read as a row
// of alarms.
//
// `outline` is by far the most-used variant here, so it is the quietest: a
// hairline and muted text that sits behind the content it annotates.
const badgeVariants = cva(
  "inline-flex w-fit shrink-0 items-center gap-1 rounded-md px-1.5 py-0.5 text-2xs font-medium leading-4 transition-colors [&_svg]:size-3 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground",
        secondary: "bg-secondary text-secondary-foreground",
        outline: "border border-border/70 bg-transparent text-muted-foreground",
        destructive: "bg-destructive-muted text-destructive ring-1 ring-inset ring-destructive/15",
        success: "bg-success-muted text-success ring-1 ring-inset ring-success/15",
        warning: "bg-warning-muted text-warning ring-1 ring-inset ring-warning/15",
        info: "bg-info-muted text-info ring-1 ring-inset ring-info/15",
        muted: "bg-muted text-muted-foreground",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

// forwardRef because several call sites wrap a Badge in `TooltipTrigger
// asChild` — Radix's Slot clones the child and attaches a ref to it, which
// throws on a plain function component.
const Badge = React.forwardRef<HTMLDivElement, BadgeProps>(({ className, variant, ...props }, ref) => (
  <div ref={ref} className={cn(badgeVariants({ variant }), className)} {...props} />
));
Badge.displayName = "Badge";

export { Badge, badgeVariants };
