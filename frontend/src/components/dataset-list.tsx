import type { DatasetSummary } from "@/lib/api";

import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useDatasets } from "@/hooks/use-datasets";
import { cn } from "@/lib/utils";

interface DatasetListProps {
  onSelect: (dataset: DatasetSummary) => void;
}

/** landing page: all registered datasets with a dot marking which have data */
export function DatasetList({ onSelect }: DatasetListProps) {
  const { data: datasets, isPending, isError, error, refetch } = useDatasets();

  if (isPending) {
    return (
      <p className="text-muted-foreground py-8 text-sm">loading datasets...</p>
    );
  }

  if (isError) {
    return (
      <div className="border-destructive/50 space-y-3 rounded-md border p-4">
        <p className="text-destructive text-sm">{error.message}</p>
        <Button variant="outline" size="sm" onClick={() => refetch()}>
          retry
        </Button>
      </div>
    );
  }

  if (datasets.length === 0) {
    return (
      <p className="text-muted-foreground py-8 text-sm">no datasets found</p>
    );
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>name</TableHead>
            <TableHead>version</TableHead>
            <TableHead>data</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {datasets.map((dataset) => (
            <TableRow
              key={`${dataset.name}@${dataset.version}`}
              className="cursor-pointer"
              onClick={() => onSelect(dataset)}
            >
              <TableCell>{dataset.name}</TableCell>
              <TableCell className="text-muted-foreground font-mono">
                {dataset.version}
              </TableCell>
              <TableCell>
                <span
                  className={cn(
                    "inline-block size-2 rounded-full",
                    dataset.has_data ? "bg-success" : "bg-border",
                  )}
                  title={dataset.has_data ? "has data" : "no data"}
                />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
