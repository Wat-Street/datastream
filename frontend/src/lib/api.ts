import { getApiKey } from "@/lib/api-key";

// dev: unset → relative /api/v1 → vite proxy. prod: absolute url baked at build time
const BASE = `${import.meta.env.VITE_API_BASE_URL ?? ""}/api/v1`;

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    // the parsed error response body, when the server sent structured json
    // beyond `detail` (e.g. proposal conflict_type/open_pr_url)
    public readonly body?: unknown,
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

async function parseErrorBody(
  res: Response,
): Promise<{ detail: string | null; body: unknown }> {
  try {
    const body: unknown = await res.json();
    const detail =
      typeof body === "object" &&
      body !== null &&
      "detail" in body &&
      typeof body.detail === "string"
        ? body.detail
        : null;
    return { detail, body };
  } catch {
    // non-json error body
    return { detail: null, body: undefined };
  }
}

async function apiFetch<T>(
  path: string,
  options: FetchOptions = {},
): Promise<T> {
  const { params, method = "GET", body, okStatuses = [200] } = options;
  const query = params ? `?${new URLSearchParams(params)}` : "";
  const key = getApiKey();
  const headers: Record<string, string> = {
    "ngrok-skip-browser-warning": "true",
  };
  if (key) headers.Authorization = `Bearer ${key}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";

  const res = await fetch(`${BASE}${path}${query}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!okStatuses.includes(res.status)) {
    const { detail, body } = await parseErrorBody(res);
    throw new ApiError(
      res.status,
      detail ?? `request to ${path} failed: ${res.status}`,
      body,
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
  // true only on a resubmit after the caller confirmed clearing a stale
  // branch/pr left over from an earlier, never-registered proposal
  override?: boolean;
}

export interface ProposalResponse {
  dataset_name: string;
  dataset_version: string;
  pr_url: string;
  branch: string;
}

// shape of a 409 ApiError.body from POST /datasets
export interface ProposalConflictBody {
  detail: string;
  conflict_type: "dataset_exists" | "stale_branch";
  open_pr_url?: string | null;
}

export function isProposalConflictBody(
  body: unknown,
): body is ProposalConflictBody {
  return typeof body === "object" && body !== null && "conflict_type" in body;
}

export interface BuildResponse {
  status: string;
}

export interface DryRunBuildResponse {
  dataset_name: string;
  dataset_version: string;
  dry_run: boolean;
  rows: DataRow[];
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

// real build: builds missing timestamps in [start, end] and writes them to the
// db. synchronous server-side, so this can take a while for large ranges
export function triggerBuild(
  name: string,
  version: string,
  start: string,
  end: string,
): Promise<BuildResponse> {
  return apiFetch<BuildResponse>(
    `/build/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
    {
      method: "POST",
      params: { start, end },
    },
  );
}

// dry run: rebuilds the whole dependency graph in-memory and returns the
// produced rows; nothing is written to the db
export function dryRunBuild(
  name: string,
  version: string,
  start: string,
  end: string,
): Promise<DryRunBuildResponse> {
  return apiFetch<DryRunBuildResponse>(
    `/build/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
    {
      method: "POST",
      params: { start, end, "dry-run": "true" },
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
