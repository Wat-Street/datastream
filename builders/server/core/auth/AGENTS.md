# Auth (`core/auth`)

API-key authentication. Every endpoint except `GET /status` requires a valid api key sent as
a bearer token:

```
Authorization: Bearer <raw-key>
```

`/status` is intentionally unauthenticated so the docker healthcheck (which hits
`/api/v1/status`) keeps working. Enforcement is wired once in `main.py`: `public_router`
(carrying `/status`) mounts open; `router` (everything else) mounts with
`dependencies=[Depends(verify_api_key)]`.

## Key storage

Valid keys live in the `API_KEYS` env var as comma-separated `label:sha256hex` pairs, e.g.
`default:ab12...,team-a:cd34...`. Only the sha256 hash of each key is stored, so a leaked
config or log never reveals a working key. The `label` identifies the caller (a team) and is
bound to the structlog context as `team` on every authenticated request. The env format is
already a `label -> hash` map, so extending from one shared key to a key-per-team is just
another entry, with no format change.

## Verification

The `verify_api_key` dependency hashes the presented token and looks it up in the key map.
Comparing the hash (not the raw key) is timing-safe: the compared value is already a sha256 of
the secret, so timing cannot recover the key.

## Fail-closed startup

The `lifespan` handler in `main.py` loads `API_KEYS` (via `load_key_map`) and raises if it is
empty, so the service refuses to start unauthenticated on a public network. There is no
"auth disabled" flag.

## Key generation

Mint a key and its env line with:

```
cd builders/server && uv run python -m core.auth generate <label>
```

This prints a `dsk_`-prefixed raw key (hand to the client) and the `label:hash` line (add to
`API_KEYS`). Rotation is a superset operation: add a new key alongside the old, roll clients
over, then remove the old entry.

## Scaling assumption

Keys are loaded from process env into an in-memory (cached) map, consistent with the
single-uvicorn-worker model used elsewhere in the service.

## Clients

The Python SDK sends the header automatically when given a key: pass
`DatastreamClient(api_key=...)` (or module-level `get_data(..., api_key=...)`), set it globally
via `configure(api_key=...)`, or export `DATASTREAM_API_KEY`. The browser frontend does not yet
send a key, so its requests currently return `401`; browser auth is handled in a later change
(see `SPEC-frontend.md`).

| Status | Meaning |
|--------|---------|
| `401` | Missing or invalid API key (on any endpoint except `/status`) |
