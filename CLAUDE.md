See @SPEC.md for the project description.

## Before writing code

Always run `gt sync` to sync the latest changes from the remote repository before writing any code. This ensures you're working with the latest version and properly restacks any open PRs.

## Tech specifications

After EVERY SET of udpates to the code, update @dev-docs/SPEC.md with what has been changed in the code.

## Plans

- Implementation plans use GitHub-flavored markdown checkboxes (`- [ ]` / `- [x]`).
- Checked items (`- [x]`) are already implemented; unchecked items (`- [ ]`) are not.
- Always include a note at the top of any plan file stating that checked items are already implemented.

## Pull requests

This repository uses Graphite for pull requests, instead of Git. **Never run commands to create or submit PRs without explicit instructions to do so.**

When explicitly instructed to make a pull request, use the Graphite skill to make a series of pull requests.

- `gt create -am` commits ALL files (tracked and untracked). To selectively commit, use `git add <files>` first, then `gt create -m "msg"` (without `-a`).
- `gt submit` does not support setting PR descriptions inline. After submitting, use `gh pr edit <number> --body "description"` to add or update PR descriptions.
- Every PR must have a description. Keep it short: what changed, why, and the benefit. Casual and concise, no fluff.

## Code styling

When writing comments other than documentation strings, do not capitalize the first letter of sentences, and do not write in sentence form

In Rust:
- Always document every function to give at least a brief explanation of what its purpose is.
    - Prioritize shorter documentation unless more complicated design is invovled.
    
