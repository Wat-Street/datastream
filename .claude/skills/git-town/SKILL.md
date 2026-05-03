---
name: git-town
description: |
  Use for git town stacked PRs workflow when git-town binary is available.
  Triggers: git town, git-town, git town hack, git town append, git town prepend,
  git town propose, git town ship, git town sync, git town up, git town down,
  git town set-parent, git town compress, git town switch, stacked PRs git town,
  sync stack git town, ship branch, propose stack, append branch, prepend branch.
  For repos WITHOUT git-town installed, use standard git commands instead.
---

# Git Town Stacked PRs Workflow

**IMPORTANT:** This workflow applies ONLY when `git-town` is installed (`which git-town`). For systems without git town, use standard git commands.

## Detection

Check for git town before using these commands:

```bash
which git-town   # present → use git town workflow
```

When git town is available, use `git town` commands for branch management. Regular `git commit` is still used for committing changes — git town handles the branch hierarchy and sync, not commits.

## Planning Stacks (CRITICAL)

**Before writing any code, present the stack structure and ask for confirmation.**

When building a feature as a stack:

1. **Plan first** — break the work into logical, sequential branches
2. **Present the structure** — show the user the planned stack:
   ```
   Branch Stack for [Feature]:
   1. Branch 1: [description] — [what it does]
   2. Branch 2: [description] — [what it does]
   3. Branch 3: [description] — [what it does]
   ```
3. **Ask for confirmation** — "Does this structure look good to proceed?"
4. **Only then start coding**

**IMPORTANT:** Each branch must be atomic and pass CI independently. Verify this before proposing.

## Command Mapping

Use these `git town` commands instead of their git equivalents:

| Instead of | Use | Purpose |
|------------|-----|---------|
| `git checkout -b <name>` | `git town hack <name>` | Create new branch off main |
| `git checkout -b <name>` (child) | `git town append <name>` | Create child branch in stack |
| `git push` | `git town sync` | Sync + push current branch |
| `git pull --rebase` | `git town sync` | Pull + rebase whole stack |
| `git checkout <branch>` | `git town switch` | Interactive branch switcher |
| `git rebase <parent>` | `git town sync` | Rebase stack onto latest parent |

Regular `git commit` and `git add` are still used normally — git town wraps branch management, not individual commits.

## Core Workflow

### Starting a feature stack

```bash
git town sync                           # sync main first
git town hack feat-auth-api             # new branch off main
# ... make changes ...
git add -A && git commit -m "feat: add auth API endpoint"
git town propose                        # open PR for this branch

git town append feat-auth-middleware    # child branch off feat-auth-api
# ... make changes ...
git add -A && git commit -m "feat: add auth middleware"
git town propose                        # PR targeting feat-auth-api

git town append feat-auth-tests
# ... add tests ...
git add -A && git commit -m "test: add auth integration tests"
git town propose --stack                # propose remaining branches in stack
```

### Shipping completed branches

```bash
# once a PR is merged:
git town sync                           # clean up merged branches, restack dependents
git town ship                           # merge current branch into parent (if not auto-merged)
```

## Editing a Branch in the Middle of a Stack

To amend or add commits to a branch that has dependent branches above it:

```bash
git town switch                         # pick the mid-stack branch interactively
# ... make changes ...
git add -A && git commit -m "fix: address review feedback"
git town sync                           # push the changes AND automatically rebase all descendant branches on top
```

`git town sync` handles the cascade — it merges the updated parent into each child branch in order, so you never have to manually rebase. If any descendant branch has conflicts, sync pauses and lets you resolve them before continuing.

To amend the most recent commit on the current branch instead of adding a new one:

```bash
git add -A && git commit --amend --no-edit   # amend last commit (use standard git)
git town sync                                 # push force + rebase dependents
```

## Branch Types

Git town supports five branch types:

| Type | Command | Behavior |
|------|---------|----------|
| **feature** (default) | `git town feature <branch>` | owned by you, synced with parent |
| **prototype** | `git town prototype <branch>` | local-only until you run `propose` |
| **parked** | `git town park <branch>` | skipped during `sync` (except if checked out) |
| **contribution** | `git town contribute <branch>` | you push commits; others manage the branch |
| **observed** | `git town observe <branch>` | read-only local copy; no push |

Use prototype for experimental work you're not ready to share. Use parked to pause work on a branch without deleting it.

## Navigation

```bash
git town branch         # view full branch hierarchy + types
git town up             # switch to child branch
git town down           # switch to parent branch
git town switch         # interactive branch picker (shows hierarchy)
```

## Sync Workflow

Run `git town sync` regularly to keep branches up to date:

```bash
git town sync           # sync current branch with parent + push
git town sync --stack   # sync current branch and all descendants
git town sync --all     # sync every local branch
```

When `git town sync` encounters conflicts, it pauses. After resolving:

```bash
git add <resolved-files>
git town continue       # resume the sync
```

To abandon:

```bash
git town undo           # undo the last git town command entirely
```

## Conflict Resolution

When sync or restack hits conflicts:

1. Check which files conflicted (`git status`)
2. Resolve conflicts in each file
3. Auto-resolvable: import order, whitespace, non-overlapping additions
4. Ask user about: same code modified differently, deleted-vs-modified, semantic conflicts
5. After resolving:
   ```bash
   git add <resolved-files>
   git town continue
   ```

To abort and return to the state before the command:

```bash
git town undo
```

## Reorganizing Stacks

Adjust stack structure as needed:

```bash
git town set-parent <branch>   # re-parent current branch onto a different branch
git town swap                  # swap current branch with its parent
git town detach                # remove from stack, make top-level branch off main
git town merge                 # merge current branch into its parent
git town compress              # squash all commits on branch into one
git town compress --stack      # squash all branches in stack
git town delete <branch>       # delete a branch and its tracking branch
git town rename <old> <new>    # rename branch + tracking branch
```

## Common Workflows

### Building a feature stack

```bash
git town sync                                          # get latest
git town hack feat-db-schema                           # start off main
git add -A && git commit -m "feat: add users table migration"
git town append feat-user-service                      # child branch
git add -A && git commit -m "feat: add user service layer"
git town append feat-user-api                          # grandchild
git add -A && git commit -m "feat: add user API endpoints"
git town propose --stack                               # propose all three
```

### Addressing review feedback on a mid-stack branch

```bash
git town switch                                        # pick the branch with feedback
# ... make fixes ...
git add -A && git commit -m "fix: address review comments"
git town sync                                          # push update + rebase all descendents
```

### Daily sync routine

```bash
git town sync --all                                    # pull main, restack, push all
# resolve any conflicts, then:
git town continue
```

### Before shipping

```bash
git town compress                                      # squash into one clean commit (optional)
git town propose                                       # open final PR
# after PR is merged:
git town sync                                          # clean up merged branches
```

## Quick Reference

See `references/cheatsheet.md` for a complete command reference.
