"""Minimal GitHub REST client for opening dataset-proposal pull requests.

The dataset-creation flow never writes to the server's own scripts directory;
it proposes new datasets as PRs so code review stays the gate for anything
that will execute on the server. This client holds the bot token and wraps
the handful of REST calls needed: read a branch sha, commit files to a new
branch (single commit via the git data api), and open the PR.
"""

import os

import requests
import structlog

logger = structlog.get_logger()

DEFAULT_API_ROOT = "https://api.github.com"
DEFAULT_REPO = "Wat-Street/datastream"
REQUEST_TIMEOUT_SECONDS = 15.0


class GitHubError(Exception):
    """Raised when a GitHub API call fails."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"github api error {status}: {message}")
        self.status = status
        self.message = message


class BranchAlreadyExistsError(GitHubError):
    """Raised when the proposal branch already exists (open proposal)."""


class GitHubClient:
    """Thin wrapper over the GitHub REST api, authenticated with a bot token."""

    def __init__(
        self,
        token: str,
        repo: str = DEFAULT_REPO,
        session: requests.Session | None = None,
        api_root: str = DEFAULT_API_ROOT,
    ) -> None:
        self.repo = repo
        self._token = token
        self._session = session or requests.Session()
        self._api_root = api_root

    @classmethod
    def from_env(cls) -> "GitHubClient":
        """Build a client from GITHUB_TOKEN / GITHUB_REPO / GITHUB_API_URL env vars.

        Raises GitHubError if no token is configured.
        """
        token = os.environ.get("GITHUB_TOKEN", "")
        if not token:
            raise GitHubError(0, "GITHUB_TOKEN is not configured on the server")
        return cls(
            token=token,
            repo=os.environ.get("GITHUB_REPO", DEFAULT_REPO),
            api_root=os.environ.get("GITHUB_API_URL", DEFAULT_API_ROOT),
        )

    def _request(self, method: str, path: str, json_body: dict | None = None) -> dict:
        res = self._session.request(
            method,
            f"{self._api_root}{path}",
            json=json_body,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if res.status_code >= 400:
            try:
                message = str(res.json().get("message", res.text))
            except ValueError:
                message = res.text
            logger.error(
                "github api call failed",
                method=method,
                path=path,
                status=res.status_code,
                message=message,
            )
            # the git refs api reports an existing branch as a 422
            if res.status_code == 422 and "already exists" in message.lower():
                raise BranchAlreadyExistsError(res.status_code, message)
            raise GitHubError(res.status_code, message)
        return dict(res.json())

    def get_branch_sha(self, branch: str) -> str:
        """Return the commit sha a branch currently points at."""
        ref = self._request("GET", f"/repos/{self.repo}/git/ref/heads/{branch}")
        return str(ref["object"]["sha"])

    def commit_files_to_new_branch(
        self,
        branch: str,
        base_sha: str,
        message: str,
        files: dict[str, str],
    ) -> str:
        """Create one commit containing `files` on a new branch off base_sha.

        `files` maps repo-relative paths to text content. Returns the commit
        sha. Raises BranchAlreadyExistsError if the branch exists.
        """
        tree = self._request(
            "POST",
            f"/repos/{self.repo}/git/trees",
            {
                "base_tree": base_sha,
                "tree": [
                    {"path": path, "mode": "100644", "type": "blob", "content": content}
                    for path, content in sorted(files.items())
                ],
            },
        )
        commit = self._request(
            "POST",
            f"/repos/{self.repo}/git/commits",
            {"message": message, "tree": tree["sha"], "parents": [base_sha]},
        )
        self._request(
            "POST",
            f"/repos/{self.repo}/git/refs",
            {"ref": f"refs/heads/{branch}", "sha": commit["sha"]},
        )
        return str(commit["sha"])

    def create_pull(self, title: str, body: str, head: str, base: str) -> dict:
        """Open a PR and return the api response (html_url, number, ...)."""
        return self._request(
            "POST",
            f"/repos/{self.repo}/pulls",
            {"title": title, "body": body, "head": head, "base": base},
        )

    def request_reviewers(self, pr_number: int, reviewers: list[str]) -> None:
        """Best-effort reviewer assignment.

        github rejects the WHOLE batch if any single login is invalid (pr
        author, non-collaborator), which would silently drop the valid
        reviewers too — so on batch failure, retry each login individually.
        Failures must not fail the proposal, so they are only logged.
        """
        try:
            self._request(
                "POST",
                f"/repos/{self.repo}/pulls/{pr_number}/requested_reviewers",
                {"reviewers": reviewers},
            )
            return
        except GitHubError as e:
            logger.warning(
                "batch reviewer request failed, retrying individually",
                pr_number=pr_number,
                reviewers=reviewers,
                error=str(e),
            )
        for reviewer in reviewers:
            try:
                self._request(
                    "POST",
                    f"/repos/{self.repo}/pulls/{pr_number}/requested_reviewers",
                    {"reviewers": [reviewer]},
                )
            except GitHubError as e:
                logger.warning(
                    "reviewer request failed",
                    pr_number=pr_number,
                    reviewer=reviewer,
                    error=str(e),
                )

    def open_pr_with_files(
        self,
        branch: str,
        base: str,
        title: str,
        body: str,
        commit_message: str,
        files: dict[str, str],
        reviewers: list[str] | None = None,
    ) -> str:
        """Commit `files` to a new branch off `base` and open a PR.

        Returns the PR's html url.
        """
        base_sha = self.get_branch_sha(base)
        self.commit_files_to_new_branch(branch, base_sha, commit_message, files)
        pr = self.create_pull(title=title, body=body, head=branch, base=base)
        if reviewers:
            # the token's own account authors the pr and cannot review it
            author = str(pr.get("user", {}).get("login", ""))
            eligible = [r for r in reviewers if r.lower() != author.lower()]
            if eligible:
                self.request_reviewers(int(pr["number"]), eligible)
        logger.info(
            "proposal pr opened",
            repo=self.repo,
            branch=branch,
            pr_url=pr.get("html_url"),
        )
        return str(pr["html_url"])
