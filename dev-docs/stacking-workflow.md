# Stacking Workflow

We use [Git Town](https://www.git-town.com/) for stacked PRs. Stack visualization is posted automatically as a comment on every PR via `git-town/action`.

## Creating a stack

```bash
git town sync                      # sync main first
git town hack feat/my-base         # branch 1 off main

# make changes, commit
git town append feat/my-feature    # branch 2 on top of branch 1

# make changes, commit
git town propose                   # open PR targeting feat/my-base (git town sets target automatically)
```

Repeat `git town append` + `git town propose` for each branch in the stack.

## Syncing when main moves

```bash
# from any branch in the stack:
git town sync --stack              # rebases entire stack from main down, force-pushes all remote branches
```

## When a parent PR merges

Once a parent branch is merged, GitHub auto-retargets the next PR to main. Locally:

```bash
git town sync --stack              # cleans up merged parent, rebases children onto main
```

## Shipping

Merge bottom-up: always merge the oldest (lowest) PR in the stack first, then work upward.

On each PR, once it's approved and CI is green: click **"Squash and merge"**.

After each merge:
```bash
git town sync --stack              # clean up locally before merging the next PR up
```

## Resolving conflicts

If a PR has conflicts after a parent merges:

```bash
git town sync --stack              # rebase against latest main
# resolve conflicts, then:
git add .
git rebase --continue
git town sync                      # force-push updated branch
```

## Command reference

| Command | When to use |
|---|---|
| `git town hack <branch>` | start new branch off main |
| `git town append <branch>` | add branch on top of current branch |
| `git town prepend <branch>` | insert branch below current branch |
| `git town sync` | sync current branch only |
| `git town sync --stack` | sync entire stack |
| `git town propose` | open or update PR with correct target |
| `git town switch` | interactive branch switcher |
| `git town compress` | squash all commits on current branch into one |
| `git town kill` | delete current branch and its PR |
| `git town diff` | show diff for current branch only (not parent) |
| `git town branch` | show current branch and position in stack |

## Rules

- Stack only branches that depend on each other. Independent work goes on separate top-level branches.
- Always ship oldest-first (bottom of stack before top).
- Never force-push to `main` or other perennial branches.
