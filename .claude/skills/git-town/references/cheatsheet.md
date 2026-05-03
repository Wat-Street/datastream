# Git Town Cheatsheet

## Branch Creation

| Command | Purpose |
|---------|---------|
| `git town hack <name>` | New branch off main |
| `git town append <name>` | New child branch (below current) |
| `git town prepend <name>` | New parent branch (above current, below current's parent) |
| `git town hack --prototype <name>` | New local-only branch off main |

## Syncing

| Command | Purpose |
|---------|---------|
| `git town sync` | Sync current branch with parent + push |
| `git town sync --stack` | Sync current + all descendant branches |
| `git town sync --all` | Sync every local branch |
| `git town continue` | Resume after resolving conflicts |
| `git town skip` | Skip current branch and continue |
| `git town undo` | Undo the last git town command |

## Proposing & Shipping

| Command | Purpose |
|---------|---------|
| `git town propose` | Open PR for current branch |
| `git town propose --stack` | Open PRs for current + descendant branches |
| `git town ship` | Merge current branch into parent |
| `git town compress` | Squash all commits on branch into one |
| `git town compress --stack` | Squash all branches in stack |

## Navigation

| Command | Purpose |
|---------|---------|
| `git town branch` | Show branch hierarchy + types |
| `git town switch` | Interactive branch picker |
| `git town up` | Switch to child branch |
| `git town down` | Switch to parent branch |

## Reorganizing

| Command | Purpose |
|---------|---------|
| `git town set-parent <branch>` | Move current branch onto a new parent |
| `git town swap` | Swap current branch with its parent |
| `git town detach` | Remove from stack, make top-level off main |
| `git town merge` | Merge current branch into its parent |
| `git town delete <branch>` | Delete branch + tracking branch |
| `git town rename <old> <new>` | Rename branch + tracking branch |

## Branch Types

| Command | Type | Behavior |
|---------|------|----------|
| `git town feature <branch>` | feature | default; owned + synced by you |
| `git town prototype <branch>` | prototype | local-only until `propose` |
| `git town park <branch>` | parked | skipped by `sync` unless checked out |
| `git town contribute <branch>` | contribution | you push; others manage parent sync |
| `git town observe <branch>` | observed | read-only; no push |

## Editing a Mid-Stack Branch

```bash
git town switch                         # navigate to the branch
git add -A && git commit -m "fix: ..." # commit changes normally
git town sync                           # push + rebase all descendant branches
```

## Standard Git Commands Still Used

Git town does NOT replace commits — use these normally:

```bash
git add -A
git commit -m "type: message"
git commit --amend --no-edit   # amend; follow with `git town sync`
git status
git diff
git log
```
