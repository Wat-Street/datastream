---
description: |
  Use for Graphite CLI stacked PRs workflow in repos with .git/.graphite_repo_config.
  Triggers: graphite, stacked PRs, dependent PRs, chained PRs, PR stack, gt create,
  gt modify, gt submit, gt sync, gt restack, gt log, gt checkout, gt up, gt down,
  rebase my stack, fix stack conflicts, split PR, land my stack, merge stack,
  sync with main/trunk, reorder branches, fold commits, amend stack, move branch
  to different parent, stack out of date, update my stack. For repos WITHOUT
  .git/.graphite_repo_config, use standard git commands instead.
---

# Graphite Stacked PRs

## Command mapping

| Instead of | Use | Purpose |
|------------|-----|---------|
| `git commit` | `gt create -am "msg"` | create new branch + commit |
| `git commit --amend` | `gt modify -a` | amend current PR |
| `git push` | `gt submit --no-interactive` | submit current + downstack |
| `git pull` | `gt sync` | pull trunk, restack, clean merged |
| `git checkout` | `gt checkout <branch>` | switch branches |
| `git rebase` | `gt restack` | rebase stack |

## Create vs amend

**`gt create -am "msg"`** — new work, new PR in the stack

**`gt modify -a`** — fixing current PR (review feedback, forgotten changes). no new message needed.

## Selective staging

`gt create -am` stages ALL files. To stage selectively:
```bash
git add <files>
gt create -m "msg"
```

## Navigation

```bash
gt log          # full stack with PR status
gt up / gt down # move through stack
gt checkout X   # jump to branch
```

## Commit message format

`type: message` — casual and concise, no fluff, no em dashes.

| Type | When to use |
|------|-------------|
| `feat:` | new feature |
| `fix:` | bug fix |
| `refactor:` | code restructuring with no behavior change |
| `test:` | adding or updating tests |
| `docs:` | documentation only |
| `perf:` | performance improvement |
| `chore:` | deps, config, tooling, other non-code changes |

## Stack workflow

```bash
gt sync                        # always start here
# ... write code ...
gt create -am "type: message"  # commit + new branch
gt submit --no-interactive     # push + open PR
gh pr edit <num> --body "..."  # set PR description
```

## Conflict resolution

After resolving conflicts:
```bash
gt continue -a   # stage all + continue restack
gt abort         # give up, return to prior state
```

Auto-resolve: import order, whitespace, non-overlapping additions.
Ask user: same code modified differently, delete vs modify, semantic conflicts.

## Inserting into a stack

`gt create --insert` creates a new branch between the current branch and its child (upstack), instead of on top:

```bash
gt checkout <parent-branch>
gt create --insert -am "type: message"  # inserts between parent and its current child
```

Use this when you realize mid-stack that a change needs to be split out into its own PR.

## Reorganizing

```bash
gt move --onto <branch>  # reparent branch
gt fold                  # merge into parent
gt upstack onto <branch> # reparent + restack descendants
```
