# Datastream Frontend

A lightweight internal UI built with Svelte + Vite, using Bun as the package manager.

- **Local dev**: `just frontend-dev` starts the Vite dev server on port 5173. The Vite config proxies `/ping` to `http://localhost:3000` (the Python FastAPI server) to avoid CORS issues.
- **Docker**: the frontend is built as static files (`bun run build`) and served by nginx on port 80. nginx proxies `/ping` to `http://builder:3000` via Docker internal DNS.
- Current functionality: a ping button that calls `GET /ping` on the Python backend and displays the status.
- The frontend is not containerized — it runs locally via `just frontend-dev` and proxies to the backend container on port 3000.
