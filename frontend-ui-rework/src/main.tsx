import { createRoot } from "react-dom/client";
// Inter Variable, bundled locally rather than fetched from a CDN — this app
// ships in a container and may run air-gapped. One import covers every weight;
// the variable font carries 100–900 in a single file.
import "@fontsource-variable/inter";
import App from "./App.tsx";
import "./index.css";

createRoot(document.getElementById("root")!).render(<App />);
