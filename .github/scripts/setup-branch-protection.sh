#!/usr/bin/env bash
# sets up branch protection for the main branch.
# prerequisites: gh CLI authenticated, run from repo root.
# usage: bash .github/scripts/setup-branch-protection.sh [branch]

set -euo pipefail

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'

BRANCH="${1:-main}"
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)

echo "Configuring branch protection for '$BRANCH' in $REPO..."

gh api \
  --method PUT \
  "repos/$REPO/branches/$BRANCH/protection" \
  --input - <<EOF
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "backend-lint",
      "backend-unit-tests",
      "backend-integration-tests",
      "frontend-precommit"
    ]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
EOF

echo ""
echo -e "${GREEN}Branch protection applied.${RESET}"
echo ""
echo -e "${CYAN}Required status checks:${RESET} backend-lint, backend-unit-tests, backend-integration-tests, frontend-precommit"
echo -e "${CYAN}Required approvals:${RESET} 1 (stale reviews dismissed)"
echo -e "${CYAN}Force push to $BRANCH:${RESET} blocked"
