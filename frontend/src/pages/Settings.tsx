import { AppLayout } from "@/components/AppLayout";
import { PlatformConnectionsPanel } from "./Connections";

const Settings = () => (
  <AppLayout
    title="Platform Connections"
    description="Configure external systems used by Data Mobility Platform"
  >
    <PlatformConnectionsPanel showHeading={false} />
  </AppLayout>
);

export default Settings;
