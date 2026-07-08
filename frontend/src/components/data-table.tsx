import type { DataRow } from "@/lib/api";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface DataTableProps {
  rows: DataRow[];
  onRowClick: (payload: Record<string, unknown>) => void;
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

/**
 * renders dataset rows with columns derived from the first entry's keys.
 * timestamps with multiple entries (e.g. one per ticker) render one <tr> per
 * entry with the timestamp cell rowspanned across them.
 */
export function DataTable({ rows, onRowClick }: DataTableProps) {
  const firstEntry = rows.find((row) => row.data.length > 0)?.data[0];

  if (rows.length === 0 || firstEntry === undefined) {
    return (
      <p className="text-muted-foreground py-8 text-center text-sm">
        no data for this range
      </p>
    );
  }

  const columns = Object.keys(firstEntry);

  return (
    <div className="overflow-x-auto rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="font-mono">timestamp</TableHead>
            {columns.map((col) => (
              <TableHead key={col} className="font-mono">
                {col}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.flatMap((row) =>
            row.data.map((entry, i) => (
              <TableRow
                key={`${row.timestamp}-${i}`}
                className="cursor-pointer"
                onClick={() =>
                  onRowClick({ timestamp: row.timestamp, ...entry })
                }
              >
                {i === 0 && (
                  <TableCell
                    rowSpan={row.data.length}
                    className="text-muted-foreground align-top font-mono"
                  >
                    {row.timestamp}
                  </TableCell>
                )}
                {columns.map((col) => (
                  <TableCell key={col} className="font-mono">
                    {formatCell(entry[col])}
                  </TableCell>
                ))}
              </TableRow>
            )),
          )}
        </TableBody>
      </Table>
    </div>
  );
}
