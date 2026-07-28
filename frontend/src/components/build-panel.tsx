import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { toast } from "sonner";

import type { BuildResponse, DataRow, DryRunBuildResponse } from "@/lib/api";

import { DataTable } from "@/components/data-table";
import { JsonModal } from "@/components/json-modal";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { dryRunBuild, triggerBuild } from "@/lib/api";
import { toISODate } from "@/lib/format";

// dry-run output is previewed inline, not paginated; cap it to keep the dom sane
const DRY_RUN_PREVIEW_LIMIT = 50;

// builds are heavier than reads, so default to a month instead of the 5-year browse window
function defaultBuildRange(): { start: string; end: string } {
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - 30);
  return { start: toISODate(start), end: toISODate(end) };
}

interface BuildPanelProps {
  name: string;
  version: string;
}

/**
 * triggers builds for a date range. real builds write missing timestamps to
 * the db and refresh the data table; dry runs preview the produced rows
 * without writing anything. dry run is the default, and a real build needs
 * confirmation before it touches the db.
 */
export function BuildPanel({ name, version }: BuildPanelProps) {
  const [startDate, setStartDate] = useState(() => defaultBuildRange().start);
  const [endDate, setEndDate] = useState(() => defaultBuildRange().end);
  const [dryRun, setDryRun] = useState(true);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [dryRunRows, setDryRunRows] = useState<DataRow[] | null>(null);
  const [selectedRow, setSelectedRow] = useState<Record<
    string,
    unknown
  > | null>(null);

  const queryClient = useQueryClient();

  const mutation = useMutation<
    BuildResponse | DryRunBuildResponse,
    Error,
    { dryRun: boolean }
  >({
    mutationFn: (opts) =>
      opts.dryRun
        ? dryRunBuild(name, version, startDate, endDate)
        : triggerBuild(name, version, startDate, endDate),
    onSuccess: (result) => {
      if ("rows" in result) {
        setDryRunRows(result.rows);
        toast.success(`dry run produced ${result.rows.length} timestamps`);
        return;
      }
      setDryRunRows(null);
      toast.success(`build complete for ${name}/${version}`);
      // refetch the data table and the has_data dot on the list view
      void queryClient.invalidateQueries({ queryKey: ["data", name, version] });
      void queryClient.invalidateQueries({ queryKey: ["datasets"] });
    },
  });

  const previewRows = dryRunRows?.slice(0, DRY_RUN_PREVIEW_LIMIT);

  // iso dates compare correctly as strings
  const rangeInverted = Boolean(startDate && endDate && endDate < startDate);
  const submitDisabled =
    mutation.isPending || !startDate || !endDate || rangeInverted;

  // dry runs are harmless and fire immediately; real builds write to the db, so
  // they go through a confirmation step first
  function handleSubmit() {
    if (dryRun) {
      mutation.mutate({ dryRun: true });
      return;
    }
    setConfirmOpen(true);
  }

  function confirmRealBuild() {
    setConfirmOpen(false);
    mutation.mutate({ dryRun: false });
  }

  return (
    <div className="space-y-3 rounded-md border p-4">
      <h3 className="text-sm font-medium">build data</h3>
      <div className="flex flex-wrap items-end gap-4">
        <div className="space-y-1.5">
          <Label htmlFor="build-start">start</Label>
          <Input
            id="build-start"
            type="date"
            value={startDate}
            max={endDate || undefined}
            onChange={(e) => setStartDate(e.target.value)}
            className="w-40"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="build-end">end</Label>
          <Input
            id="build-end"
            type="date"
            value={endDate}
            min={startDate || undefined}
            onChange={(e) => setEndDate(e.target.value)}
            className="w-40"
          />
        </div>
        <div className="flex items-center gap-2 pb-2.5">
          <Checkbox
            id="build-dry-run"
            checked={dryRun}
            onCheckedChange={(checked) => setDryRun(checked === true)}
          />
          <Label htmlFor="build-dry-run">dry run</Label>
        </div>
        <Button
          size="sm"
          className="mb-0.5"
          disabled={submitDisabled}
          onClick={handleSubmit}
        >
          {mutation.isPending
            ? "building..."
            : dryRun
              ? "preview build"
              : "build"}
        </Button>
      </div>

      {rangeInverted && (
        <p className="text-destructive text-sm">
          end date must be on or after the start date
        </p>
      )}

      {mutation.isPending && (
        <p className="text-muted-foreground text-sm">
          builds run synchronously on the server — large ranges can take a
          while...
        </p>
      )}

      {mutation.isError && (
        <div className="border-destructive/50 rounded-md border p-3">
          <p className="text-destructive text-sm">{mutation.error.message}</p>
        </div>
      )}

      {previewRows && dryRunRows && (
        <div className="space-y-2">
          <p className="text-muted-foreground text-sm">
            dry run produced {dryRunRows.length} timestamps — nothing was
            written to the database
            {dryRunRows.length > DRY_RUN_PREVIEW_LIMIT &&
              ` (showing first ${DRY_RUN_PREVIEW_LIMIT})`}
          </p>
          <DataTable rows={previewRows} onRowClick={setSelectedRow} />
        </div>
      )}

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>run a real build?</DialogTitle>
            <DialogDescription>
              this builds {name}/{version} from {startDate} to {endDate} and
              writes the produced rows to the database. already-built timestamps
              are skipped, but new rows can't be removed from the ui. tick
              &ldquo;dry run&rdquo; instead to preview the output first.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setConfirmOpen(false)}
            >
              cancel
            </Button>
            <Button size="sm" onClick={confirmRealBuild}>
              build for real
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <JsonModal data={selectedRow} onClose={() => setSelectedRow(null)} />
    </div>
  );
}
