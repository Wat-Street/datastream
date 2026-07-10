import {
  MutationCache,
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { toast } from "sonner";

import App from "@/App";
import { Toaster } from "@/components/ui/sonner";
import { ApiKeyProvider, requestApiKey } from "@/hooks/use-api-key";
import { ApiError } from "@/lib/api";

import "@/index.css";

function handleAuthError(error: unknown) {
  if (error instanceof ApiError && error.status === 401) {
    toast.error("invalid or missing API key");
    requestApiKey();
  }
}

const queryClient = new QueryClient({
  queryCache: new QueryCache({ onError: handleAuthError }),
  mutationCache: new MutationCache({ onError: handleAuthError }),
  defaultOptions: {
    queries: {
      // client errors won't fix themselves on retry
      retry: (failureCount, error) => {
        if (
          error instanceof ApiError &&
          [401, 403, 404].includes(error.status)
        ) {
          return false;
        }
        return failureCount < 2;
      },
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ApiKeyProvider>
        <App />
        <Toaster />
      </ApiKeyProvider>
    </QueryClientProvider>
  </StrictMode>,
);
