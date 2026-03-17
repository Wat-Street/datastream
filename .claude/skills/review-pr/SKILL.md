---
description: Use when addressing PR review feedback, responding to inline comments, or fixing issues flagged in a code review.
disable-model-invocation: true
---

# Review PR Skill

## Step 1 — Fetch the review data

Get a quick overview first:

```bash
gh pr view <number> --json reviews,reviewDecision,comments
```

Then drill into inline comments if needed:

```bash
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
PR=<number>

# general review summaries (state + body text)
gh api repos/$REPO/pulls/$PR/reviews

# line-specific inline comments
gh api repos/$REPO/pulls/$PR/comments
```

## Step 2 — Navigate to the right branch

```bash
gt log               # see the full stack with PR status
gt checkout <branch> # switch to the branch that needs fixing
```

## Step 3 — Address the feedback

Choose based on what the fix involves:

- **Fix to existing code in this PR** → amend the commit (no new message needed):
  ```bash
  gt modify -a
  ```

- **New work that extends scope or belongs in a new PR** → create a new commit:
  ```bash
  gt create -am "type: message"
  ```

## Step 4 — Re-submit

```bash
gt submit --no-interactive  # pushes the current branch and restacks dependents
```

## Step 5 — Update PR description if needed

```bash
gh pr edit <pr-number> --body "new description"
```
