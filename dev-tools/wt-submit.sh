#!/usr/bin/env bash
set -euo pipefail

# wrapper around gt submit that auto-cleans _wt-* worktree branches from the stack.
# gt delete --force restacks children onto parent (main), so the first real
# PR branch ends up directly on main with no phantom empty branch.

current=$(git branch --show-current)

if [[ "$current" == _wt-* ]]; then
    echo "error: still on worktree branch '$current'. run 'gt create' first." >&2
    exit 1
fi

# find _wt-* branch in current stack
wt_branch=""
while IFS= read -r line; do
    # strip unicode bullets, spaces, and marker characters
    branch=$(echo "$line" | sed 's/^[^a-zA-Z0-9_]*//')
    if [[ "$branch" == _wt-* ]]; then
        wt_branch="$branch"
        break
    fi
done < <(gt log short --no-interactive --stack 2>/dev/null)

# if worktree branch found, delete it (auto-restacks children onto main)
if [[ -n "$wt_branch" ]]; then
    echo "cleaning up worktree branch: $wt_branch"
    gt delete "$wt_branch" --force --no-interactive
fi

gt submit --no-interactive
