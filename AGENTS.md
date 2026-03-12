See @dev-docs/SPEC-backend.md (backend) and @dev-docs/SPEC-frontend.md (frontend) for technical specs.

## Core Rules
- **Graphite Only:** Always use `gt` (Graphite) instead of `git` for branch/PR management.
- **Sync First:** Always run `gt sync` before writing any code.
- **Specs:** Update the relevant spec file in `@dev-docs/` after every code change.
- **No Manual PRs:** Never create or submit PRs unless explicitly instructed.

## Commands
Use `just` for routine tasks:
- `just lint` — ruff check
- `just test` — pytest
- `just build-rs` — build Rust API
- `just clippy` — Rust clippy (warnings as errors)
- `just precommit` — all pre-commit hooks

## Workflow
1. **Plan:** Define the full PR stack (architectural layers: Schema -> Logic -> API -> Specs). Get approval before coding.
2. **Commit:** Use conventional commits: `type: [feature] message`. Casual and concise.
3. **Sequence:**
   - `gt sync`
   - `gt create -am "type: message"` (or `git add` + `gt create -m`)
   - `gt submit --no-interactive`
   - `gh pr edit <number> --body "what/why/benefit"` (Short, no fluff, no em dashes).

## Styling
- **Comments:** No capitalization, no sentence form (unless docstrings).
- **Python:** `# type: ignore` must have an inline explanation; use plain functions for tests (no classes).
- **Rust:** Every function must have brief documentation explaining its purpose.
