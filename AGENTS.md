See @dev-docs/SPEC-backend.md for the backend spec and @dev-docs/SPEC-frontend.md for the frontend spec.

## Before writing code

Always run `gt sync` to sync the latest changes from the remote repository before writing any code. This ensures you're working with the latest version and properly restacks any open PRs.

## Common commands

Use the `just` commands defined in the `Justfile` for routine dev tasks:

- `just fix` — run ruff linter with autofix and formatter (`uv run ruff check --fix && uv run ruff format`)
- `just test` — run the test suite (`uv run pytest`)
- `just precommit` — run all pre-commit hooks (`uv run pre-commit run --all-files`)
- `just docker-up` — build and start all Docker containers
- `just docker-down` — stop all Docker containers
- `just frontend-dev` — start the Vite dev server for the frontend
- `just migrate` — apply all pending DB migrations (`uv run alembic upgrade head`)
- `just migrate-new NAME` — create a new migration revision
- `just migrate-down` — revert the last migration
- `just migrate-history` — show migration history
- `just worktree-create NAME` — create a new git worktree with env files and Graphite tracking
- `just worktree-remove NAME` — remove a worktree and clean up its branch
- `just submit` — submit PRs, auto-cleaning worktree branches

## Dev workflow

### Planning

Before writing any code for a feature or fix, always plan the full PR stack upfront:
1. Identify all PRs needed and their order. **STRICTLY prioritize making PRs as small and easily digestible as possible.** Break up larger PRs into smaller ones whenever possible or appropriate to facilitate easier reviews.
2. Present the stack plan to the user and get approval
3. Only then begin writing code

### PR stack structure

Structure stacks by architectural layer, bottom-up:
1. Schema / data model changes
2. Business logic / service layer
3. API / interface layer
4. If there is a significant spec change: update the relevant spec file (`SPEC-backend.md` or `SPEC-frontend.md`) as the final PR

Each PR in a stack should make one logical change. It is acceptable — and sometimes desirable — for a later PR to overwrite or refine what an earlier PR did. This is intentional: the stack shows the logical progression to the final state, not just the diff.

### Commit message format

All PR titles must follow conventional commits format: `type: message`

Common types:
- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation changes
- `refactor:` code refactoring
- `test:` test additions or updates
- `perf:` performance improvements
- `chore:` other changes (deps, config, etc.)

Examples:
- `feat: add lookback support to builder dependencies`
- `fix: correct timestamp validation in dataset schema`
- `docs: update builder script examples in README`
- `test: add integration tests for dependency resolution`

Messages are casual and concise, no fluff.

### Command sequence for each PR

1. `gt sync` — sync from trunk and restack (already required before writing code)
2. Write the code for this PR
3. `gt create -am "type: message"` — stage all and create branch+commit
4. `just submit` — push and open the PR (auto-cleans worktree branches)
5. `gh pr edit <number> --body "description"` — add PR description (what changed, why, benefit)

Repeat steps 2-5 for each PR in the stack. See the `graphite` skill for full command reference.

### Worktree workflow

To work on parallel stacks, use worktrees:

```
just worktree-create myfeature
cd ../myfeature
# make changes
gt create -am "feat: thing"
# more changes
gt create -am "feat: other"
just submit
```

`worktree-create` handles env file copying and Graphite branch tracking. `just submit` transparently cleans up the worktree branch before submitting so PRs stack directly on main.

## Tech specifications

After EVERY SET of updates to the code, update the relevant spec file (@dev-docs/SPEC-backend.md or @dev-docs/SPEC-frontend.md) with what has been changed in the code.

## Pull requests

This repository uses Graphite for pull requests. **Never run commands to create or submit PRs without explicit instructions to do so.**

Always use `gt` commands — never `git commit`, `git push`, or `gh pr create` directly. When instructed to make PRs, use the `graphite` skill.

Every PR must have a description (`gh pr edit <number> --body "..."`): what changed, why, and the benefit. Escape backticks with `\`` in body strings.

## Code styling

When writing comments other than documentation strings, do not capitalize the first letter of sentences, and do not write in sentence form

In Python:
- Every `# type: ignore` must have an inline comment explaining why the suppression is safe.
- Use plain functions for tests, not class-based test grouping.

In Rust:
- Always document every function to give at least a brief explanation of what its purpose is.
    - Prioritize shorter documentation unless more complicated design is invovled.
