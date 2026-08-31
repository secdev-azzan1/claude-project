import type { Config } from "tailwindcss";
import tailwindcssAnimate from "tailwindcss-animate";

export default {
  darkMode: ["class"],
  content: ["./pages/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}", "./app/**/*.{ts,tsx}", "./src/**/*.{ts,tsx}"],
  prefix: "",
  theme: {
    container: {
      center: true,
      padding: "2rem",
      screens: {
        "2xl": "1400px",
      },
    },
    extend: {
      fontFamily: {
        // Inter is the closest widely-available stand-in for SF Pro, and it is
        // bundled locally (@fontsource-variable/inter) rather than pulled from a
        // CDN — this app ships in a container and may run air-gapped. The system
        // stack behind it means a machine that HAS SF Pro uses the real thing.
        sans: [
          "Inter Variable",
          "Inter",
          "-apple-system",
          "BlinkMacSystemFont",
          "SF Pro Text",
          "Segoe UI",
          "Roboto",
          "Helvetica Neue",
          "Arial",
          "sans-serif",
        ],
        mono: [
          "SF Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Cascadia Mono",
          "Consolas",
          "Liberation Mono",
          "monospace",
        ],
      },
      // A deliberately short scale. The old build ran nearly everything at
      // text-xs, which is why secondary content never receded — there was no
      // step below it to recede TO. Sizes are paired with their line-height and
      // with optical tracking: large type tightens, small type opens up.
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem", letterSpacing: "0.006em" }], // 11 — chips, keycaps
        xs: ["0.75rem", { lineHeight: "1.0625rem", letterSpacing: "0.003em" }], // 12 — metadata
        sm: ["0.8125rem", { lineHeight: "1.25rem" }], // 13 — secondary / controls
        base: ["0.9375rem", { lineHeight: "1.4375rem" }], // 15 — body
        lg: ["1.0625rem", { lineHeight: "1.5rem", letterSpacing: "-0.008em" }], // 17 — section titles
        xl: ["1.1875rem", { lineHeight: "1.625rem", letterSpacing: "-0.012em" }], // 19
        "2xl": ["1.375rem", { lineHeight: "1.75rem", letterSpacing: "-0.016em" }], // 22 — page titles
        "3xl": ["1.75rem", { lineHeight: "2.125rem", letterSpacing: "-0.021em" }], // 28
        "4xl": ["2.125rem", { lineHeight: "2.5rem", letterSpacing: "-0.024em" }], // 34
        "5xl": ["2.75rem", { lineHeight: "3rem", letterSpacing: "-0.026em" }], // 44
      },
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
          muted: "hsl(var(--primary-muted))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
          muted: "hsl(var(--destructive-muted))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        success: {
          DEFAULT: "hsl(var(--success))",
          foreground: "hsl(var(--success-foreground))",
          muted: "hsl(var(--success-muted))",
        },
        warning: {
          DEFAULT: "hsl(var(--warning))",
          foreground: "hsl(var(--warning-foreground))",
          muted: "hsl(var(--warning-muted))",
        },
        info: {
          DEFAULT: "hsl(var(--info))",
          foreground: "hsl(var(--info-foreground))",
          muted: "hsl(var(--info-muted))",
        },
        popover: {
          DEFAULT: "hsl(var(--popover))",
          foreground: "hsl(var(--popover-foreground))",
        },
        card: {
          DEFAULT: "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
        sidebar: {
          DEFAULT: "hsl(var(--sidebar-background))",
          foreground: "hsl(var(--sidebar-foreground))",
          primary: "hsl(var(--sidebar-primary))",
          "primary-foreground": "hsl(var(--sidebar-primary-foreground))",
          accent: "hsl(var(--sidebar-accent))",
          "accent-foreground": "hsl(var(--sidebar-accent-foreground))",
          border: "hsl(var(--sidebar-border))",
          ring: "hsl(var(--sidebar-ring))",
        },
      },
      // Elevation scale. These OVERRIDE the stock keys on purpose: tailwind-merge
      // classifies `shadow-<unknown>` as a shadow *color*, so a custom key like
      // `shadow-e1` would not dedupe against `shadow-sm` in cn() and CSS source
      // order would pick the winner. Values come from src/index.css so `.dark`
      // can redefine the whole ramp (a flat black shadow is invisible on
      // --card: 240 6% 11%).
      boxShadow: {
        sm: "var(--elevation-1)",
        DEFAULT: "var(--elevation-2)",
        md: "var(--elevation-3)",
        lg: "var(--elevation-4)",
        xl: "var(--elevation-5)",
        "2xl": "var(--elevation-6)",
        inner: "var(--elevation-inset)",
        none: "none",
      },
      borderRadius: {
        // --radius is 12px. The ramp below it is what controls inside a card
        // use, so a nested control never out-rounds its container.
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
        xl: "calc(var(--radius) + 4px)",
        "2xl": "calc(var(--radius) + 10px)",
        "3xl": "calc(var(--radius) + 18px)",
      },
      transitionTimingFunction: {
        // The standard iOS/macOS curve: leaves fast, settles slow.
        spring: "cubic-bezier(0.32, 0.72, 0, 1)",
        "out-quart": "cubic-bezier(0.25, 1, 0.5, 1)",
      },
      keyframes: {
        "accordion-down": {
          from: { height: "0", opacity: "0" },
          to: { height: "var(--radix-accordion-content-height)", opacity: "1" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)", opacity: "1" },
          to: { height: "0", opacity: "0" },
        },
        // Overlays scale up from 98% rather than appearing flat — the small
        // scale delta is most of what makes a macOS panel feel like it arrived
        // rather than switched on.
        "overlay-in": {
          from: { opacity: "0", transform: "scale(0.98)" },
          to: { opacity: "1", transform: "scale(1)" },
        },
        "overlay-out": {
          from: { opacity: "1", transform: "scale(1)" },
          to: { opacity: "0", transform: "scale(0.98)" },
        },
        "fade-in": {
          from: { opacity: "0" },
          to: { opacity: "1" },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.24s cubic-bezier(0.32, 0.72, 0, 1)",
        "accordion-up": "accordion-up 0.2s cubic-bezier(0.32, 0.72, 0, 1)",
        "overlay-in": "overlay-in 0.2s cubic-bezier(0.32, 0.72, 0, 1)",
        "overlay-out": "overlay-out 0.15s ease-in",
        "fade-in": "fade-in 0.2s ease-out",
      },
    },
  },
  plugins: [tailwindcssAnimate],
} satisfies Config;
