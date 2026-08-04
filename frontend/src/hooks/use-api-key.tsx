import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

import { clearStoredApiKey, getApiKey, storeApiKey } from "@/lib/api-key";

interface ApiKeyContextValue {
  apiKey: string | null;
  saveApiKey: (key: string) => void;
  clearApiKey: () => void;
  dialogOpen: boolean;
  setDialogOpen: (open: boolean) => void;
}

const ApiKeyContext = createContext<ApiKeyContextValue | null>(null);

// module-level hook so non-react code (the query client's error handler)
// can pop the key dialog
let openDialogListener: (() => void) | null = null;

export function requestApiKey(): void {
  openDialogListener?.();
}

export function ApiKeyProvider({ children }: { children: ReactNode }) {
  const [apiKey, setApiKey] = useState<string | null>(() => getApiKey());
  // auto-open on first load when no key is stored: the backend rejects
  // everything except /status without one
  const [dialogOpen, setDialogOpen] = useState<boolean>(() => !getApiKey());

  useEffect(() => {
    openDialogListener = () => setDialogOpen(true);
    return () => {
      openDialogListener = null;
    };
  }, []);

  const saveApiKey = useCallback((key: string) => {
    storeApiKey(key);
    setApiKey(key);
  }, []);

  const clearApiKey = useCallback(() => {
    clearStoredApiKey();
    setApiKey(null);
  }, []);

  return (
    <ApiKeyContext.Provider
      value={{ apiKey, saveApiKey, clearApiKey, dialogOpen, setDialogOpen }}
    >
      {children}
    </ApiKeyContext.Provider>
  );
}

export function useApiKey(): ApiKeyContextValue {
  const ctx = useContext(ApiKeyContext);
  if (!ctx) {
    throw new Error("useApiKey must be used inside ApiKeyProvider");
  }
  return ctx;
}
