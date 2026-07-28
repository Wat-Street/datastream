import { KeyRoundIcon, PlusIcon } from "lucide-react";
import { lazy, Suspense, useState } from "react";

import type { DatasetSummary } from "@/lib/api";

import { ApiKeyDialog } from "@/components/api-key-dialog";
import { DatasetDetail } from "@/components/dataset-detail";
import { DatasetList } from "@/components/dataset-list";
import { Button } from "@/components/ui/button";
import { useApiKey } from "@/hooks/use-api-key";

// lazy: the create view pulls in codemirror, which shouldn't weigh down browsing
const CreateDataset = lazy(() =>
  import("@/components/create-dataset").then((module) => ({
    default: module.CreateDataset,
  })),
);

type View =
  | { view: "list" }
  | { view: "detail"; dataset: DatasetSummary }
  | { view: "create" };

function App() {
  const { setDialogOpen } = useApiKey();
  const [view, setView] = useState<View>({ view: "list" });

  return (
    <div className="mx-auto max-w-[1200px] px-8 py-6">
      <header className="mb-8 flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Datastream</h1>
        <div className="flex items-center gap-2">
          {view.view === "list" && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setView({ view: "create" })}
            >
              <PlusIcon /> new dataset
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setDialogOpen(true)}
            className="text-muted-foreground"
          >
            <KeyRoundIcon /> API key
          </Button>
        </div>
      </header>
      <main>
        {view.view === "list" && (
          <DatasetList
            onSelect={(dataset) => setView({ view: "detail", dataset })}
          />
        )}
        {view.view === "detail" && (
          <DatasetDetail
            name={view.dataset.name}
            version={view.dataset.version}
            onBack={() => setView({ view: "list" })}
          />
        )}
        {view.view === "create" && (
          <Suspense
            fallback={
              <p className="text-muted-foreground py-8 text-sm">loading...</p>
            }
          >
            <CreateDataset onBack={() => setView({ view: "list" })} />
          </Suspense>
        )}
      </main>
      <ApiKeyDialog />
    </div>
  );
}

export default App;
