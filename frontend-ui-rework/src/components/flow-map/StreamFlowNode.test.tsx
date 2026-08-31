import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import { StreamFlowNodeBody, type StreamNodeData } from "@/components/flow-map/StreamFlowNode";

const baseData = (over: Partial<StreamNodeData> = {}): StreamNodeData => ({
  name: "put_items",
  endpointPath: "/api/items/id",
  isEntity: false,
  isRoot: false,
  isRouteChild: true,
  isBranchChild: false,
  hasRouteChildren: false,
  hasParallelChildren: false,
  childCount: 1,
  selected: false,
  onSelect: vi.fn(),
  onAddBranch: vi.fn(),
  ...over,
});

describe("StreamFlowNodeBody", () => {
  it("renders the name, path and a Route badge", () => {
    render(<StreamFlowNodeBody id="s1" data={baseData()} />);
    expect(screen.getByText("put_items")).toBeInTheDocument();
    expect(screen.getByText("/api/items/id")).toBeInTheDocument();
    expect(screen.getByText("Route")).toBeInTheDocument();
  });

  it("calls onAddBranch (and not onSelect) when + is clicked", () => {
    const onSelect = vi.fn();
    const onAddBranch = vi.fn();
    render(<StreamFlowNodeBody id="s1" data={baseData({ onSelect, onAddBranch })} />);
    fireEvent.click(screen.getByLabelText("Add parallel branch"));
    expect(onAddBranch).toHaveBeenCalledWith("s1");
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("calls onSelect when the card body is clicked", () => {
    const onSelect = vi.fn();
    render(<StreamFlowNodeBody id="s1" data={baseData({ onSelect })} />);
    fireEvent.click(screen.getByText("put_items"));
    expect(onSelect).toHaveBeenCalledWith("s1");
  });
});
