import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

// Alerts in this app carry lock reasons, drift findings and rule refusals — the
// text has to stay verbatim, so the styling has to do the de-escalating instead.
// Each variant is a soft tint with a hairline ring, not a saturated slab: it
// reads as a marked passage rather than an interruption.
const alertVariants = cva(
  "relative w-full rounded-xl border px-4 py-3 text-sm [&>svg]:absolute [&>svg]:left-4 [&>svg]:top-3.5 [&>svg]:size-4 [&>svg~*]:pl-7",
  {
    variants: {
      variant: {
        default: "border-border/60 bg-muted/50 text-foreground [&>svg]:text-muted-foreground",
        destructive:
          "border-destructive/20 bg-destructive-muted text-destructive [&>svg]:text-destructive [&_p]:text-destructive/85",
        warning: "border-warning/20 bg-warning-muted text-warning [&>svg]:text-warning [&_p]:text-warning/85",
        success: "border-success/20 bg-success-muted text-success [&>svg]:text-success [&_p]:text-success/85",
        info: "border-info/20 bg-info-muted text-info [&>svg]:text-info [&_p]:text-info/85",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

const Alert = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & VariantProps<typeof alertVariants>
>(({ className, variant, ...props }, ref) => (
  <div ref={ref} role="alert" className={cn(alertVariants({ variant }), className)} {...props} />
));
Alert.displayName = "Alert";

const AlertTitle = React.forwardRef<HTMLParagraphElement, React.HTMLAttributes<HTMLHeadingElement>>(
  ({ className, ...props }, ref) => (
    <h5 ref={ref} className={cn("mb-0.5 text-sm font-semibold leading-tight tracking-tight", className)} {...props} />
  ),
);
AlertTitle.displayName = "AlertTitle";

const AlertDescription = React.forwardRef<HTMLParagraphElement, React.HTMLAttributes<HTMLParagraphElement>>(
  ({ className, ...props }, ref) => (
    <div ref={ref} className={cn("text-sm leading-relaxed opacity-90 [&_p]:leading-relaxed", className)} {...props} />
  ),
);
AlertDescription.displayName = "AlertDescription";

export { Alert, AlertTitle, AlertDescription };
