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

interface FetchOptions {
  params?: Record<string, string>;
  method?: string;
  body?: unknown;
  okStatuses?: number[];
}

async function extractDetail(res: Response): Promise<string | null> {
  try {
    const body: unknown = await res.json();
    if (
      typeof body === "object" &&
      body !== null &&
      "detail" in body &&
      typeof body.detail === "string"
    ) {
      return body.detail;
    }
  } catch {
    // non-json error body
  }
  return null;
}

async function apiFetch<T>(
  path: string,
  options: FetchOptions = {},
): Promise<T> {
  const { params, method = "GET", body, okStatuses = [200] } = options;
  const query = params ? `?${new URLSearchParams(params)}` : "";
  const key = getApiKey();
  const headers: Record<string, string> = {};
  if (key) headers.Authorization = `Bearer ${key}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";

  const res = await fetch(`${BASE}${path}${query}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!okStatuses.includes(res.status)) {
    const detail = await extractDetail(res);
    throw new ApiError(
      res.status,
      detail ?? `request to ${path} failed: ${res.status}`,
    );
  }
  return res.json() as Promise<T>;
}

export interface ProposedDependency {
  name: string;
  version: string;
  lookback?: string;
}

export interface DatasetProposalPayload {
  name: string;
  version: string;
  calendar: string;
  granularity: string;
  start_date: string;
  schema: Record<string, string>;
  dependencies: ProposedDependency[];
  builder_script: string;
  author_name: string;
  team: string;
  discord_user: string;
  description: string;
  env_vars: boolean;
  requirements_txt?: string;
  env_template?: string;
}

export interface ProposalResponse {
  dataset_name: string;
  dataset_version: string;
  pr_url: string;
  branch: string;
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
    {
      params: { start, end, "build-data": "false" },
      okStatuses: [200, 206],
    },
  );
}

// nothing is written to the server: the backend validates the submission and
// opens a github pr; the dataset goes live after review + merge + restart
export function proposeDataset(
  payload: DatasetProposalPayload,
): Promise<ProposalResponse> {
  return apiFetch<ProposalResponse>("/datasets", {
    method: "POST",
    body: payload,
  });
}
