import { useQuery } from "@tanstack/react-query";

import { fetchDatasets } from "@/lib/api";

export function useDatasets() {
  return useQuery({
    queryKey: ["datasets"],
    queryFn: fetchDatasets,
  });
}
