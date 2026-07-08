import { KeyRoundIcon } from "lucide-react";
import { useState } from "react";

import type { DatasetSummary } from "@/lib/api";

import { ApiKeyDialog } from "@/components/api-key-dialog";
import { DatasetDetail } from "@/components/dataset-detail";
import { DatasetList } from "@/components/dataset-list";
import { Button } from "@/components/ui/button";
import { useApiKey } from "@/hooks/use-api-key";

type View = { view: "list" } | { view: "detail"; dataset: DatasetSummary };

function App() {
  const { setDialogOpen } = useApiKey();
  const [view, setView] = useState<View>({ view: "list" });

  return (
    <div className="mx-auto max-w-[1200px] px-8 py-6">
      <header className="mb-8 flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Datastream</h1>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setDialogOpen(true)}
          className="text-muted-foreground"
        >
          <KeyRoundIcon /> API key
        </Button>
      </header>
      <main>
        {view.view === "list" ? (
          <DatasetList
            onSelect={(dataset) => setView({ view: "detail", dataset })}
          />
        ) : (
          <DatasetDetail
            name={view.dataset.name}
            version={view.dataset.version}
            onBack={() => setView({ view: "list" })}
          />
        )}
      </main>
      <ApiKeyDialog />
    </div>
  );
}

export default App;
