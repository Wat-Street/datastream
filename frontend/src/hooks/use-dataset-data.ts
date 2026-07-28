import { useQuery } from "@tanstack/react-query";

import { fetchData } from "@/lib/api";

export function useDatasetData(
  name: string,
  version: string,
  start: string,
  end: string,
) {
  return useQuery({
    queryKey: ["data", name, version, start, end],
    queryFn: () => fetchData(name, version, start, end),
  });
}
