import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/StatusBadge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Download, Search, Loader2, RefreshCw } from "lucide-react";
import { listAudit } from "@/prototype/api";
import type { AuditEvent } from "@/prototype/types";
import { useQueryClient } from "@tanstack/react-query";
import { useDebouncedValue } from "@/lib/useDebouncedValue";

const formatTimestamp = (iso: string) => {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
};

const Audit = () => {
  const queryClient = useQueryClient();
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebouncedValue(search, 400);

  const { data, isLoading, error } = useQuery<AuditEvent[]>({
    queryKey: ["audit", debouncedSearch],
    queryFn: () => listAudit(debouncedSearch),
    refetchInterval: 15_000,
  });

  const exportCsv = () => {
    if (!data) return;
    const rows = [
      ["Timestamp", "User", "Action", "Object", "Target", "Status", "Details"],
      ...data.map((e) => [
        e.ts,
        e.user,
        e.action,
        e.object,
        e.target,
        e.status,
        e.details || "",
      ]),
    ];
    const csv = rows.map((r) => r.map((c) => `"${c}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `audit-log-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <AppLayout
      title="Audit Log"
      description="Immutable history of admin actions"
      actions={
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => queryClient.invalidateQueries({ queryKey: ["audit"] })}>
            <RefreshCw className="h-4 w-4" />
          </Button>
          <Button variant="outline" onClick={exportCsv} disabled={!data?.length}>
            <Download className="h-4 w-4" /> Export CSV
          </Button>
        </div>
      }
    >
      <Card className="mb-4">
        <CardContent className="p-3 flex flex-wrap gap-2">
          <div className="relative flex-1 min-w-48">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Filter actions, objects, targets..."
              className="pl-8 h-9"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          {data && (
            <span className="flex items-center text-xs text-muted-foreground px-2">
              {data.length} event{data.length !== 1 ? "s" : ""}
            </span>
          )}
        </CardContent>
      </Card>

      {error && (
        <div className="rounded-md bg-destructive/10 border border-destructive/20 p-4 text-sm text-destructive mb-4">
          Failed to load audit log: {(error as Error).message}
        </div>
      )}

      <Card>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center h-40">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Timestamp</TableHead>
                  <TableHead>User</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Object</TableHead>
                  <TableHead>Target</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data?.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                      No audit events found
                    </TableCell>
                  </TableRow>
                ) : (
                  data?.map((e) => (
                    <TableRow key={e.id}>
                      <TableCell className="font-mono text-xs whitespace-nowrap">
                        {formatTimestamp(e.ts)}
                      </TableCell>
                      <TableCell>
                        <span className="inline-flex items-center gap-2">
                          <span className="h-5 w-5 rounded-full bg-accent text-accent-foreground text-xs flex items-center justify-center font-semibold">
                            {e.user.charAt(0).toUpperCase()}
                          </span>
                          {e.user}
                        </span>
                      </TableCell>
                      <TableCell className="font-medium">{e.action}</TableCell>
                      <TableCell className="text-muted-foreground">{e.object}</TableCell>
                      <TableCell className="font-mono text-xs">{e.target}</TableCell>
                      <TableCell><StatusBadge status={e.status} /></TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </AppLayout>
  );
};

export default Audit;
