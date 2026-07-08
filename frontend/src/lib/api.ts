import { getApiKey } from "@/lib/api-key";

// dev: unset → relative /api/v1 → vite proxy. prod: absolute url baked at build time
const BASE = `${import.meta.env.VITE_API_BASE_URL ?? ""}/api/v1`;

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export interface DatasetSummary {
  name: string;
  version: string;
  has_data: boolean;
}

export interface DataRow {
  timestamp: string;
  data: Record<string, unknown>[];
}

export interface DataResponse {
  dataset_name: string;
  dataset_version: string;
  total_timestamps: number;
  returned_timestamps: number;
  rows: DataRow[];
}

async function apiFetch<T>(
  path: string,
  params?: Record<string, string>,
  okStatuses: number[] = [200],
): Promise<T> {
  const query = params ? `?${new URLSearchParams(params)}` : "";
  const key = getApiKey();
  const res = await fetch(`${BASE}${path}${query}`, {
    headers: key ? { Authorization: `Bearer ${key}` } : {},
  });
  if (!okStatuses.includes(res.status)) {
    throw new ApiError(res.status, `request to ${path} failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export async function fetchDatasets(): Promise<DatasetSummary[]> {
  const body = await apiFetch<{ datasets: DatasetSummary[] }>("/datasets");
  return body.datasets;
}

// 206 = partial data, still a valid read; build-data=false so browsing never triggers builds
export function fetchData(
  name: string,
  version: string,
  start: string,
  end: string,
): Promise<DataResponse> {
  return apiFetch<DataResponse>(
    `/data/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
    { start, end, "build-data": "false" },
    [200, 206],
  );
}
