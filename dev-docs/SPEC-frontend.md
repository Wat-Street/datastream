# Datastream Frontend

A lightweight internal UI built with React 19 + TypeScript + Vite, using Bun as the package manager and JS runtime (`bunx --bun vite`, so the installer and runtime always agree on native-binding architecture).

- **Local dev**: `just frontend-dev` starts the Vite dev server on port 5173. The Vite config proxies `/api` to `http://localhost:3000` (the Python FastAPI server) to avoid CORS issues.
- **Production**: deployed to **GitHub Pages** at `https://wat-street.github.io/datastream/` by `.github/workflows/deploy-pages.yml` (see "Deployment" below). The frontend is not containerized and nothing in `infra/` serves it — Caddy only proxies the API.

## Design

This is a purely internal tool. The goal is functional clarity, not aesthetics.

- **Tailwind CSS v4 + shadcn/ui** (new-york style, CSS variables) — theme tokens live in `src/index.css`
- **Dark-only theme** — the dark palette is set directly on `:root`; no theme toggle, no `.dark` class
- **No custom fonts** — system font stack (Inter fallback to system sans-serif, JetBrains Mono fallback to system monospace)
- **Layout** — single-column, max-width 1200px centered; no sidebars or complex layouts

## Architecture

### Navigation

State-based navigation in `App.tsx`. Two views:
- `'list'`: dataset catalog (landing page)
- `'detail'`: dataset data viewer

No routing library. A single discriminated-union `view` state controls which component renders. This also means GitHub Pages needs no SPA 404 fallback.

### File structure

```
frontend/src/
  main.tsx                       # entrypoint: QueryClient (central 401 handling), providers, mount
  index.css                      # tailwind v4 theme: shadcn tokens + json highlight colors
  App.tsx                        # root component: header, api-key button, view switching
  lib/
    api.ts                       # typed fetch client for /api/v1 (ApiError, bearer header)
    api-key.ts                   # localStorage key storage + masking helper
    format.ts                    # toISODate, defaultDateRange (5-year window), formatTimestamp
    utils.ts                     # shadcn cn() helper
  hooks/
    use-api-key.tsx              # ApiKeyProvider context + requestApiKey() bridge for non-react code
    use-datasets.ts              # TanStack Query hook for GET /datasets
    use-dataset-data.ts          # TanStack Query hook for GET /data
  components/
    ui/                          # shadcn-generated components (button, table, dialog, form, ...)
    dataset-list.tsx             # landing page: all datasets + has_data dot
    dataset-detail.tsx           # detail view: paginated data table (50/page, newest first)
    data-table.tsx               # dynamic-column table with rowspan for multi-entry timestamps
    json-view.tsx                # recursive jsx syntax highlighter (no innerHTML)
    json-modal.tsx               # row details in a shadcn Dialog
    api-key-dialog.tsx           # zod + react-hook-form key entry dialog
```

### Dependencies

Runtime: React 19, TanStack Query (fetch state, caching, retries, central error handling), react-hook-form + zod + `@hookform/resolvers` (forms/validation), shadcn/ui's underlying packages (`radix-ui`, `lucide-react`, `sonner`, `class-variance-authority`, `clsx`, `tailwind-merge`).

### Tooling and quality gates

Frontend developer tooling is managed in `frontend/package.json` with Bun scripts (same script names as the previous Svelte app, so pre-commit hooks and CI are unchanged):

- `bun run format:check` / `format:write` -- Prettier (with the tailwind class-sorting plugin)
- `bun run lint` -- ESLint (flat config: typescript-eslint + react-hooks + react-refresh)
- `bun run typecheck` -- `tsc -b` (project references: app + node configs)
- `bun run build` -- Vite production build (under the bun runtime)

Repository git hooks are managed via root `.pre-commit-config.yaml` and split by stage:

- **pre-commit** (fast checks): `frontend-format-check`, `frontend-eslint`
- **pre-push** (heavier checks): `frontend-typecheck`, `frontend-build`

CI mirrors local frontend gates in `.github/workflows/frontend-ci.yml` (installs Bun, `bun install --frozen-lockfile`, runs the hook ids for both stages).

### API integration

All API calls go through `lib/api.ts` and use the `/api/v1` prefix.

- **Base URL**: `VITE_API_BASE_URL` (baked in at build time, set for Pages builds) or relative `/api` in dev, where Vite proxies to localhost:3000
- `fetchDatasets()` -- `GET /api/v1/datasets`
- `fetchData(name, version, start, end)` -- `GET /api/v1/data/{name}/{version}?start=...&end=...&build-data=false`

The frontend is read-only and never triggers builds (`build-data=false` always). Both 200 and 206 responses are treated as valid (206 indicates partial/incomplete data). Failures throw `ApiError` (carries the HTTP status) so the query layer can react per-status.

### API-key auth

The backend requires `Authorization: Bearer <dsk_...>` on everything except `/status`. Since Pages is a static host, the key is supplied by the user:

- The **api-key dialog** (zod-validated, `dsk_` prefix) auto-opens on first load when no key is stored, and can be reopened anytime from the header button. It shows a masked view of the stored key and offers a clear button.
- The key is stored in **localStorage** (`datastream.api_key`) and attached to every request by `lib/api.ts`. Saving a key invalidates all queries so they refetch immediately.
- **Central 401 handling**: the QueryClient's `QueryCache.onError` turns any 401 into a toast and reopens the dialog. Retries skip 401/403/404.
- Caveat: localStorage is readable by any script on the page (XSS). Accepted for an internal tool; the JSX-based `json-view` (no `innerHTML` anywhere) is part of keeping that risk low.

## Deployment (GitHub Pages)

`.github/workflows/deploy-pages.yml` runs on pushes to `main` touching `frontend/**` (or manual dispatch): bun install + build, then `actions/deploy-pages` publishes `frontend/dist`.

- `vite.config.ts` sets `base: "/datastream/"` for production builds only; local dev stays at `/`
- One-time repo setup: **Settings → Pages → Source = "GitHub Actions"**, and an Actions **variable** `VITE_API_BASE_URL` pointing at the https API origin (the Caddy `DOMAIN`; no trailing slash, no `/api/v1`)
- Pages serves over https, so the API origin must also be https (mixed content is blocked; localhost is exempt)
- The API must allow the Pages origin via CORS — see "CORS" in `SPEC-backend.md`

## Features

### Dataset list (landing page)

`dataset-list.tsx` fetches all datasets and displays them in a table with columns: name, version, and a green/gray dot indicating whether data exists in the database. Rows are clickable and navigate to the detail view. Handles loading, error (with retry button), and empty states.

### Dataset detail view

`dataset-detail.tsx` shows data for a single dataset with client-side pagination.

- **Auto-fetch**: fetches all existing data on mount (5-year window, `build-data=false`)
- **Pagination**: 50 rows per page, newest data first. "newer" / "older" buttons to navigate pages
- **Metadata**: shows returned timestamp count and current page number
- **Error handling**: error banner with retry button

### Data table

`data-table.tsx` renders dataset rows as an HTML table with dynamically derived column headers (computed from the keys of the first data entry).

- Multi-row timestamps (e.g. multiple tickers at the same timestamp) use `rowspan` on the timestamp cell
- All rows are clickable and open the JSON modal
- Horizontally scrollable for wide datasets

### JSON detail modal

`json-modal.tsx` displays a clicked row's data as syntax-highlighted JSON in a shadcn Dialog.

- Highlighting is done by `json-view.tsx`, a recursive JSX renderer (colors keys, strings, numbers, booleans, null) — no HTML strings, no `dangerouslySetInnerHTML`
- Closes on: Escape key, backdrop click, or close button (built into the Dialog)

## Planned (not yet implemented)

- **Create-dataset UI**: a form for name/version/calendar/granularity/start-date/schema/dependencies plus a builder-script editor (CodeMirror). Blocked on approval of the corresponding backend endpoint, which changes the builder trust model (script upload = code execution gated by API key instead of git review).
