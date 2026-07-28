import { ArrowLeftIcon } from "lucide-react";
import { useMemo, useState } from "react";

import { BuildPanel } from "@/components/build-panel";
import { DataTable } from "@/components/data-table";
import { JsonModal } from "@/components/json-modal";
import { Button } from "@/components/ui/button";
import { useDatasetData } from "@/hooks/use-dataset-data";
import { defaultDateRange } from "@/lib/format";

const PAGE_SIZE = 50;

interface DatasetDetailProps {
  name: string;
  version: string;
  onBack: () => void;
}

/** detail view: paginated data table over a 5-year window, newest first */
export function DatasetDetail({ name, version, onBack }: DatasetDetailProps) {
  // freeze the range for the lifetime of the view so the query key is stable
  const [range] = useState(() => defaultDateRange());
  const [page, setPage] = useState(0);
  const [selectedRow, setSelectedRow] = useState<Record<
    string,
    unknown
  > | null>(null);

  const { data, isPending, isError, error, refetch } = useDatasetData(
    name,
    version,
    range.start,
    range.end,
  );

  // newest first
  const allRows = useMemo(() => (data ? [...data.rows].reverse() : []), [data]);
  const totalPages = Math.max(1, Math.ceil(allRows.length / PAGE_SIZE));
  const pageRows = allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div className="space-y-4">
      <Button
        variant="ghost"
        size="sm"
        onClick={onBack}
        className="text-muted-foreground -ml-2"
      >
        <ArrowLeftIcon /> back
      </Button>
      <h2 className="text-xl font-semibold">
        {name}{" "}
        <span className="text-muted-foreground font-mono text-base font-normal">
          {version}
        </span>
      </h2>

      <BuildPanel name={name} version={version} />

      {isPending && (
        <p className="text-muted-foreground py-8 text-sm">loading data...</p>
      )}

      {isError && (
        <div className="border-destructive/50 space-y-3 rounded-md border p-4">
          <p className="text-destructive text-sm">{error.message}</p>
          <Button variant="outline" size="sm" onClick={() => refetch()}>
            retry
          </Button>
        </div>
      )}

      {data && (
        <>
          <p className="text-muted-foreground text-sm">
            {data.returned_timestamps} timestamps · page {page + 1} of{" "}
            {totalPages}
          </p>
          <DataTable rows={pageRows} onRowClick={setSelectedRow} />
          {totalPages > 1 && (
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                size="sm"
                disabled={page === 0}
                onClick={() => setPage((p) => p - 1)}
              >
                ← newer
              </Button>
              <span className="text-muted-foreground text-sm">
                page {page + 1} / {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages - 1}
                onClick={() => setPage((p) => p + 1)}
              >
                older →
              </Button>
            </div>
          )}
        </>
      )}

      <JsonModal data={selectedRow} onClose={() => setSelectedRow(null)} />
    </div>
  );
}
