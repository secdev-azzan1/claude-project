import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

import { StreamFlowMap } from "@/components/flow-map/StreamFlowMap";
import { buildStreamGraph } from "@/lib/streamGraph";

const streams = [
  { id: "a", name: "list_items", endpointPath: "/api/items" },
  { id: "b", name: "put_items", endpointPath: "/api/items/id", parentStreamId: "a" },
];

describe("StreamFlowMap", () => {
  it("renders one node per stream", async () => {
    const graph = buildStreamGraph(streams);
    render(
      <div style={{ width: 800, height: 400 }}>
        <StreamFlowMap
          streams={streams}
          graph={graph}
          editingStream="a"
          setEditingStream={vi.fn()}
          onCreateParallelBranch={vi.fn()}
        />
      </div>,
    );
    expect(await screen.findByText("list_items")).toBeInTheDocument();
    expect(await screen.findByText("put_items")).toBeInTheDocument();
  });
});
