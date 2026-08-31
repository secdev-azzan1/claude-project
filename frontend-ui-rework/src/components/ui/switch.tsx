import * as React from "react";
import * as SwitchPrimitives from "@radix-ui/react-switch";

import { cn } from "@/lib/utils";

// Proportioned like the iOS switch: a wide track with a thumb that nearly fills
// it and casts a real shadow, rather than a small pill floating in a large one.
// The spring easing on the thumb is what makes it feel thrown rather than moved.
const Switch = React.forwardRef<
  React.ElementRef<typeof SwitchPrimitives.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitives.Root>
>(({ className, ...props }, ref) => (
  <SwitchPrimitives.Root
    className={cn(
      "peer inline-flex h-[26px] w-[44px] shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent p-0 transition-colors duration-200",
      "data-[state=checked]:bg-primary data-[state=unchecked]:bg-muted-foreground/30",
      "focus-visible:outline-none focus-visible:ring-[3px] focus-visible:ring-ring/35",
      "disabled:cursor-not-allowed disabled:opacity-45",
      className,
    )}
    {...props}
    ref={ref}
  >
    <SwitchPrimitives.Thumb
      className={cn(
        "pointer-events-none block h-[22px] w-[22px] rounded-full bg-white shadow-[0_1px_2px_rgba(0,0,0,0.18),0_2px_6px_rgba(0,0,0,0.12)] ring-0",
        "transition-transform duration-200 ease-spring",
        "data-[state=checked]:translate-x-[18px] data-[state=unchecked]:translate-x-0",
      )}
    />
  </SwitchPrimitives.Root>
));
Switch.displayName = SwitchPrimitives.Root.displayName;

export { Switch };
