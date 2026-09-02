import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import Dashboard from "./pages/Dashboard.tsx";
import AppServices from "./pages/AppServices.tsx";
import Connections from "./pages/Connections.tsx";
import FlowBuilder from "./pages/FlowBuilder.tsx";
import Schemas from "./pages/Schemas.tsx";
import Flows from "./pages/Flows.tsx";
import Apisix from "./pages/Apisix.tsx";
import Audit from "./pages/Audit.tsx";
import NotFound from "./pages/NotFound.tsx";
import { FlowOperationQueue } from "./components/FlowOperationQueue";

const queryClient = new QueryClient();

const App = () => (
  // The `.dark` palette was fully authored but unreachable — nothing ever put
  // the class on <html>. `disableTransitionOnChange` stops every surface on the
  // page from cross-fading its colours at once when the theme flips, which reads
  // as a glitch rather than a transition.
  <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={200}>
        <Toaster />
        <Sonner position="bottom-left" />
        <FlowOperationQueue />
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/flows" element={<Flows />} />
            <Route path="/flow-builder/new" element={<FlowBuilder />} />
            <Route path="/flow-builder/:flowId" element={<FlowBuilder />} />
            <Route path="/schemas" element={<Schemas />} />
            <Route path="/application-services" element={<AppServices />} />
            <Route path="/audit" element={<Audit />} />
            <Route path="/connections" element={<Connections />} />
            <Route path="/apisix" element={<Apisix />} />
            {/* Legacy routes from the stream-based app */}
            {/* Kafka Connect sinks are now operated from the Sync tab on each flow */}
            <Route path="/kafka-connect" element={<Navigate to="/flows" replace />} />
            <Route path="/flow-designer" element={<Navigate to="/flows" replace />} />
            <Route path="/nifi-services" element={<Navigate to="/application-services" replace />} />
            <Route path="/settings" element={<Navigate to="/connections" replace />} />
            <Route path="/settings/connections" element={<Navigate to="/connections" replace />} />
            {/* Global variables were removed — per-flow variables live in Flow settings */}
            <Route path="/variables" element={<Navigate to="/" replace />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </ThemeProvider>
);

export default App;
