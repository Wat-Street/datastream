import { KeyRoundIcon } from "lucide-react";

import { ApiKeyDialog } from "@/components/api-key-dialog";
import { Button } from "@/components/ui/button";
import { useApiKey } from "@/hooks/use-api-key";

function App() {
  const { setDialogOpen } = useApiKey();

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
      <main />
      <ApiKeyDialog />
    </div>
  );
}

export default App;
