import json

import pytest
import requests
from core.github.client import (
    BranchAlreadyExistsError,
    GitHubClient,
    GitHubError,
)
from requests.adapters import BaseAdapter


class FakeAdapter(BaseAdapter):
    """records outgoing requests and plays back canned (status, body) responses."""

    def __init__(self, responses: list[tuple[int, dict]]):
        super().__init__()
        self.responses = list(responses)
        self.sent: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):  # noqa: ANN001 -- test double
        self.sent.append(request)
        status, body = self.responses.pop(0)
        res = requests.Response()
        res.status_code = status
        res._content = json.dumps(body).encode()
        res.headers["Content-Type"] = "application/json"
        res.request = request
        return res

    def close(self):
        pass


def _client(
    responses: list[tuple[int, dict]],
) -> tuple[GitHubClient, FakeAdapter]:
    session = requests.Session()
    adapter = FakeAdapter(responses)
    session.mount("https://", adapter)
    return GitHubClient(token="tok", repo="acme/data", session=session), adapter


def _body(request: requests.PreparedRequest) -> dict:
    assert request.body is not None
    return dict(json.loads(request.body))


def test_open_pr_with_files_happy_path() -> None:
    """the full flow: read base sha, tree, commit, branch ref, pr."""
    client, adapter = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (201, {"ref": "refs/heads/add-dataset/foo-0.1.0"}),
            (201, {"html_url": "https://github.com/acme/data/pull/7", "number": 7}),
        ]
    )

    url = client.open_pr_with_files(
        branch="add-dataset/foo-0.1.0",
        base="main",
        title="feat: add dataset foo/0.1.0",
        body="body",
        commit_message="feat: add dataset foo/0.1.0",
        files={"b/two.py": "print(2)\n", "a/one.toml": "x = 1\n"},
    )

    assert url == "https://github.com/acme/data/pull/7"
    paths = [(req.method, req.path_url) for req in adapter.sent]
    assert paths == [
        ("GET", "/repos/acme/data/git/ref/heads/main"),
        ("POST", "/repos/acme/data/git/trees"),
        ("POST", "/repos/acme/data/git/commits"),
        ("POST", "/repos/acme/data/git/refs"),
        ("POST", "/repos/acme/data/pulls"),
    ]

    assert adapter.sent[0].headers["Authorization"] == "Bearer tok"

    tree_body = _body(adapter.sent[1])
    assert tree_body["base_tree"] == "base-sha"
    # files are sorted by path for deterministic trees
    assert [entry["path"] for entry in tree_body["tree"]] == ["a/one.toml", "b/two.py"]

    commit_body = _body(adapter.sent[2])
    assert commit_body == {
        "message": "feat: add dataset foo/0.1.0",
        "tree": "tree-sha",
        "parents": ["base-sha"],
    }

    ref_body = _body(adapter.sent[3])
    assert ref_body == {
        "ref": "refs/heads/add-dataset/foo-0.1.0",
        "sha": "commit-sha",
    }

    pr_body = _body(adapter.sent[4])
    assert pr_body["head"] == "add-dataset/foo-0.1.0"
    assert pr_body["base"] == "main"


_PR_CREATED = (
    201,
    {
        "html_url": "https://github.com/acme/data/pull/7",
        "number": 7,
        "user": {"login": "bot-account"},
    },
)


def test_reviewers_requested_after_pr_created() -> None:
    client, adapter = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (201, {"ref": "refs/heads/b"}),
            _PR_CREATED,
            (201, {"requested_reviewers": []}),
        ]
    )
    client.open_pr_with_files(
        branch="b",
        base="main",
        title="t",
        body="b",
        commit_message="m",
        files={"a": "1"},
        reviewers=["alice", "bob"],
    )
    assert adapter.sent[-1].path_url == "/repos/acme/data/pulls/7/requested_reviewers"
    assert _body(adapter.sent[-1]) == {"reviewers": ["alice", "bob"]}


def test_pr_author_excluded_from_reviewers() -> None:
    """the token's account cannot review its own pr; it is filtered out."""
    client, adapter = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (201, {"ref": "refs/heads/b"}),
            _PR_CREATED,
            (201, {"requested_reviewers": []}),
        ]
    )
    client.open_pr_with_files(
        branch="b",
        base="main",
        title="t",
        body="b",
        commit_message="m",
        files={"a": "1"},
        reviewers=["alice", "Bot-Account"],
    )
    assert _body(adapter.sent[-1]) == {"reviewers": ["alice"]}


def test_author_only_reviewer_list_skips_request() -> None:
    client, adapter = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (201, {"ref": "refs/heads/b"}),
            _PR_CREATED,
        ]
    )
    client.open_pr_with_files(
        branch="b",
        base="main",
        title="t",
        body="b",
        commit_message="m",
        files={"a": "1"},
        reviewers=["bot-account"],
    )
    # no reviewer call was made: the pr creation is the last request
    assert adapter.sent[-1].path_url == "/repos/acme/data/pulls"


def test_batch_reviewer_failure_retries_individually() -> None:
    """one invalid login must not drop the valid reviewers with it."""
    client, adapter = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (201, {"ref": "refs/heads/b"}),
            _PR_CREATED,
            (422, {"message": "Reviews may only be requested from collaborators."}),
            (201, {"requested_reviewers": []}),
            (422, {"message": "Reviews may only be requested from collaborators."}),
        ]
    )
    url = client.open_pr_with_files(
        branch="b",
        base="main",
        title="t",
        body="b",
        commit_message="m",
        files={"a": "1"},
        reviewers=["alice", "not-a-collaborator"],
    )
    assert url == "https://github.com/acme/data/pull/7"
    # batch failed, then each reviewer was retried on its own
    reviewer_calls = [
        _body(req)
        for req in adapter.sent
        if req.path_url.endswith("/requested_reviewers")
    ]
    assert reviewer_calls == [
        {"reviewers": ["alice", "not-a-collaborator"]},
        {"reviewers": ["alice"]},
        {"reviewers": ["not-a-collaborator"]},
    ]


def test_api_error_carries_status_and_message() -> None:
    client, _ = _client([(404, {"message": "Not Found"})])
    with pytest.raises(GitHubError) as exc_info:
        client.get_branch_sha("main")
    assert exc_info.value.status == 404
    assert "Not Found" in exc_info.value.message


def test_existing_branch_raises_specific_error() -> None:
    """the git refs api reports an existing branch as a 422."""
    client, _ = _client(
        [
            (200, {"object": {"sha": "base-sha"}}),
            (201, {"sha": "tree-sha"}),
            (201, {"sha": "commit-sha"}),
            (422, {"message": "Reference already exists"}),
        ]
    )
    with pytest.raises(BranchAlreadyExistsError):
        client.open_pr_with_files(
            branch="add-dataset/foo-0.1.0",
            base="main",
            title="t",
            body="b",
            commit_message="m",
            files={"a": "1"},
        )


def test_from_env_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(GitHubError):
        GitHubClient.from_env()


def test_from_env_reads_repo_and_api_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPO", "acme/data")
    monkeypatch.setenv("GITHUB_API_URL", "http://localhost:9999")
    client = GitHubClient.from_env()
    assert client.repo == "acme/data"
    assert client._api_root == "http://localhost:9999"
