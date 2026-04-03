# Datastream Frontend

A lightweight internal UI built with Svelte 5 + Vite, using Bun as the package manager.

- **Local dev**: `just frontend-dev` starts the Vite dev server on port 5173. The Vite config proxies `/api` to `http://localhost:3000` (the Python FastAPI server) to avoid CORS issues.
- **Docker**: the frontend is built as static files (`bun run build`) and served by nginx on port 80. nginx proxies `/api` to `http://builder:3000` via Docker internal DNS.
- The frontend is not containerized -- it runs locally via `just frontend-dev` and proxies to the backend container on port 3000.

## Design

This is a purely internal tool. The goal is functional clarity, not aesthetics.

- **No CSS framework or component library** -- scoped Svelte `<style>` blocks + a global `app.css` for resets and CSS custom properties
- **Dark theme** -- dark background with light text, using CSS custom properties for all colors
- **No custom fonts** -- system font stack (Inter fallback to system sans-serif, JetBrains Mono fallback to system monospace)
- **Layout** -- single-column, max-width 1200px centered; no sidebars or complex layouts
- **Spacing** -- CSS custom properties (`--space-xs` through `--space-xl`) for consistent spacing
- **Interactive states** -- subtle hover transitions on clickable rows and buttons

## Architecture

### Navigation

State-based navigation in `App.svelte` using Svelte 5 runes. Two views:
- `'list'`: dataset catalog (landing page)
- `'detail'`: dataset data viewer

No routing library. `view` and `selectedDataset` state variables control which component renders.

### File structure

```
frontend/src/
  main.js                        # entrypoint, mounts App and imports global CSS
  app.css                        # CSS reset, custom properties, base styles
  App.svelte                     # root component: header + view switching
  lib/
    api.js                       # fetch wrappers for /api/v1 endpoints
    format.js                    # date formatting, JSON syntax highlighting
  components/
    DatasetList.svelte           # landing page: fetches and lists all datasets
    DatasetDetail.svelte         # detail view: date range controls + data table + modal
    DataTable.svelte             # dynamic-column HTML table with rowspan support
    JsonModal.svelte             # modal overlay with syntax-highlighted JSON
```

### Dependencies

Zero additional runtime dependencies beyond Svelte 5 and Vite.

### API integration

All API calls go through `lib/api.js` and use the `/api/v1` prefix (proxied to the backend by Vite in dev).

- `fetchDatasets()` -- `GET /api/v1/datasets`
- `fetchData(name, version, start, end)` -- `GET /api/v1/data/{name}/{version}?start=...&end=...&build-data=false`

The frontend is read-only and never triggers builds (`build-data=false` always). Both 200 and 206 responses are treated as valid (206 indicates partial/incomplete data).

## Features

### Dataset list (landing page)

`DatasetList.svelte` fetches all datasets on mount and displays them in a table with columns: name, version, and a green/gray dot indicating whether data exists in the database. Rows are clickable and navigate to the detail view. Handles loading, error (with retry button), and empty states.

### Dataset detail view

`DatasetDetail.svelte` shows data for a single dataset in a configurable time range.

- **Date range controls**: two native `<input type="date">` fields (defaulting to the last 30 days) and a "Load" button
- **Auto-fetch**: loads data automatically on mount with the default range
- **Completeness indicator**: "showing X of Y timestamps" from the API response metadata
- **Error handling**: error banner with retry button

### Data table

`DataTable.svelte` renders dataset rows as an HTML table with dynamically derived column headers (computed from the keys of the first data entry).

- Multi-row timestamps (e.g. multiple tickers at the same timestamp) use `rowspan` on the timestamp cell
- All rows are clickable and open the JSON modal
- Horizontally scrollable for wide datasets

### JSON detail modal

`JsonModal.svelte` displays a clicked row's data as syntax-highlighted JSON in a modal overlay.

- CSS-based syntax highlighting using `highlightJson()` from `lib/format.js` -- colors keys, strings, numbers, booleans, and null values
- Closes on: Escape key, backdrop click, or close button
- Scrollable for large data objects
