See @dev-docs/SPEC-backend.md for the backend spec and @dev-docs/SPEC-frontend.md for the frontend spec.

## Before writing code

Always run `gt sync` to sync the latest changes from the remote repository before writing any code. This ensures you're working with the latest version and properly restacks any open PRs.

## Common commands

Use the `just` commands defined in the `Justfile` for routine dev tasks:

- `just lint` — run ruff linter (`uv run ruff check`)
- `just test` — run the test suite (`uv run pytest`)
- `just build-rs` — build the Rust API (`cd api && cargo build`)
- `just clippy` — run clippy with warnings as errors (`cd api && cargo clippy -- -D warnings`)
- `just precommit` — run all pre-commit hooks (`uv run pre-commit run --all-files`)

## Dev workflow

### Planning

Before writing any code for a feature or fix, always plan the full PR stack upfront:
1. Identify all PRs needed and their order
2. Present the stack plan to the user and get approval
3. Only then begin writing code

### PR stack structure

Structure stacks by architectural layer, bottom-up:
1. Schema / data model changes
2. Business logic / service layer
3. API / interface layer
4. If there is a significant spec change: update the relevant spec file (`SPEC-backend.md` or `SPEC-frontend.md`) as the final PR

Each PR in a stack should make one logical change. It is acceptable — and sometimes desirable — for a later PR to overwrite or refine what an earlier PR did. This is intentional: the stack shows the logical progression to the final state, not just the diff.

### Command sequence for each PR

1. `gt sync` — sync from trunk and restack (already required before writing code)
2. Write the code for this PR
3. `gt create -am "type: message"` — stage all and create branch+commit (or `git add <files>` + `gt create -m "..."` to commit selectively)
4. `gt submit --no-interactive` — push and open the PR
5. `gh pr edit <number> --body "description"` — add PR description (what changed, why, benefit)

Repeat steps 2–5 for each PR in the stack.

### PR description

Every PR must have a description set via `gh pr edit`. Keep it short and casual:
- What changed
- Why
- The benefit

No fluff, no em dashes.

## Tech specifications

After EVERY SET of updates to the code, update the relevant spec file (@dev-docs/SPEC-backend.md or @dev-docs/SPEC-frontend.md) with what has been changed in the code.

## Pull requests

This repository uses Graphite for pull requests, instead of Git. **Never run commands to create or submit PRs without explicit instructions to do so.**

**Always use `gt` commands over `git` commands.** PRs must ONLY ever be created and submitted using Graphite (`gt create`, `gt submit`). Never use `git commit`, `git push`, or `gh pr create` directly.

When explicitly instructed to make a pull request, use the Graphite skill to make a series of pull requests.

- `gt create -am` commits ALL files (tracked and untracked). To selectively commit, use `git add <files>` first, then `gt create -m "msg"` (without `-a`).
- `gt submit` does not support setting PR descriptions inline. After submitting, use `gh pr edit <number> --body "description"` to add or update PR descriptions.
- Every PR must have a description. Keep it short: what changed, why, and the benefit. Casual and concise, no fluff.

## Code styling

When writing comments other than documentation strings, do not capitalize the first letter of sentences, and do not write in sentence form

In Python:
- Every `# type: ignore` must have an inline comment explaining why the suppression is safe.
- Use plain functions for tests, not class-based test grouping.

In Rust:
- Always document every function to give at least a brief explanation of what its purpose is.
    - Prioritize shorter documentation unless more complicated design is invovled.
    
