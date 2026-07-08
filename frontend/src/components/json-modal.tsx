import { JsonView } from "@/components/json-view";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

interface JsonModalProps {
  data: Record<string, unknown> | null;
  onClose: () => void;
}

/** modal showing one row's data as syntax-highlighted json */
export function JsonModal({ data, onClose }: JsonModalProps) {
  return (
    <Dialog open={data !== null} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-h-[80vh] sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Row data</DialogTitle>
          <DialogDescription className="sr-only">
            JSON contents of the selected row
          </DialogDescription>
        </DialogHeader>
        <pre className="overflow-auto rounded-md border p-4 font-mono text-sm">
          <code>{data !== null && <JsonView value={data} />}</code>
        </pre>
      </DialogContent>
    </Dialog>
  );
}
