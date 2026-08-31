import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { StatusBadge } from "@/components/StatusBadge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { AlertCircle, Download, Search, Loader2, RefreshCw, ScrollText } from "lucide-react";
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
      ...data.map((e) => [e.ts, e.user, e.action, e.object, e.target, e.status, e.details || ""]),
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
        <>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="icon-sm"
                onClick={() => queryClient.invalidateQueries({ queryKey: ["audit"] })}
              >
                <RefreshCw />
                <span className="sr-only">Refresh</span>
              </Button>
            </TooltipTrigger>
            <TooltipContent>Refresh</TooltipContent>
          </Tooltip>
          <Button variant="outline" size="sm" onClick={exportCsv} disabled={!data?.length}>
            <Download /> Export CSV
          </Button>
        </>
      }
    >
      {error && (
        <Alert variant="destructive" className="mb-4">
          <AlertCircle />
          <AlertTitle>Failed to load audit log</AlertTitle>
          <AlertDescription>{(error as Error).message}</AlertDescription>
        </Alert>
      )}

      {/* Search and the table are one object, so they share one card rather than
          sitting in two stacked panels with a gap between them. */}
      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-center gap-3 border-b border-border/60 p-3">
          <div className="relative min-w-56 flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground/70" />
            <Input
              placeholder="Filter actions, objects, targets…"
              className="h-9 pl-9"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          {data && (
            <span className="shrink-0 px-1 text-xs tabular-nums text-muted-foreground">
              {data.length} event{data.length !== 1 ? "s" : ""}
            </span>
          )}
        </div>

        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex h-40 items-center justify-center">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="pl-4">Timestamp</TableHead>
                  <TableHead>User</TableHead>
                  <TableHead>Action</TableHead>
                  <TableHead>Object</TableHead>
                  <TableHead>Target</TableHead>
                  <TableHead className="pr-4">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data?.length === 0 ? (
                  <TableRow className="hover:bg-transparent">
                    <TableCell colSpan={6} className="py-14 text-center">
                      <div className="flex flex-col items-center gap-3">
                        <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-muted">
                          <ScrollText className="h-5 w-5 text-muted-foreground/70" />
                        </div>
                        <p className="text-sm text-muted-foreground">No audit events found</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  data?.map((e) => (
                    <TableRow key={e.id}>
                      <TableCell className="whitespace-nowrap pl-4 font-mono text-xs text-muted-foreground">
                        {formatTimestamp(e.ts)}
                      </TableCell>
                      <TableCell>
                        <span className="inline-flex items-center gap-2 whitespace-nowrap">
                          <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-primary-muted text-2xs font-semibold text-primary">
                            {e.user.charAt(0).toUpperCase()}
                          </span>
                          {e.user}
                        </span>
                      </TableCell>
                      <TableCell className="font-medium">{e.action}</TableCell>
                      <TableCell className="text-muted-foreground">{e.object}</TableCell>
                      <TableCell className="max-w-[18rem] truncate font-mono text-xs" title={e.target}>
                        {e.target}
                      </TableCell>
                      <TableCell className="pr-4">
                        <StatusBadge status={e.status} />
                      </TableCell>
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
