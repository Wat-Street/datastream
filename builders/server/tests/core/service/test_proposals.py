import subprocess
import tomllib
from pathlib import Path
from typing import Any

import core.runtime.registry as registry
import core.service.proposals as proposals
import pytest
from core.github.client import BranchAlreadyExistsError
from core.service.proposals import (
    DatasetProposal,
    InvalidProposalError,
    ProposalConflictError,
    ProposedDependency,
    StaleProposalConflictError,
    generate_config_toml,
    propose_dataset,
)

_DEP_CONFIG = """\
name = "mock-dep"
version = "0.1.0"
granularity = "1d"
start-date = "2021-06-01"
calendar = "everyday"

[schema]
price = "int"
"""

VALID_BUILDER = """\
from datetime import datetime


def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"ticker": "AAPL", "price": 1}]
"""


class FakeGitHub:
    """stands in for GitHubClient; records the pr call or raises.

    `error` is only raised on the *first* open_pr_with_files call, so tests
    can exercise the override path's clear-then-retry without a second fake.
    """

    def __init__(
        self,
        error: Exception | None = None,
        open_pr: dict[str, Any] | None = None,
    ):
        self.error = error
        self.open_pr = open_pr
        self.calls: list[dict[str, Any]] = []
        self.closed_prs: list[int] = []
        self.deleted_branches: list[str] = []

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
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        self.calls.append(
            {
                "branch": branch,
                "base": base,
                "title": title,
                "body": body,
                "commit_message": commit_message,
                "files": files,
                "reviewers": reviewers,
            }
        )
        return "https://github.com/acme/data/pull/42"

    def find_open_pull_for_branch(self, branch: str, base: str) -> dict | None:
        return self.open_pr

    def close_pull(self, pr_number: int) -> None:
        self.closed_prs.append(pr_number)

    def delete_branch(self, branch: str) -> None:
        self.deleted_branches.append(branch)


@pytest.fixture(autouse=True)
def _registry_with_dep(tmp_path: Path):
    """populate the registry with mock-dep/0.1.0; reset afterwards."""
    dep_dir = tmp_path / "mock-dep" / "0.1.0"
    dep_dir.mkdir(parents=True)
    (dep_dir / "config.toml").write_text(_DEP_CONFIG)
    registry.load_all_configs(tmp_path)
    yield
    registry._CONFIG_REGISTRY = {}


def _proposal(**overrides: Any) -> DatasetProposal:
    defaults: dict[str, Any] = dict(
        name="my-dataset",
        version="0.1.0",
        calendar="everyday",
        granularity="1d",
        start_date="2022-01-01",
        schema={"ticker": "str", "price": "float"},
        builder_script=VALID_BUILDER,
        author_name="Kai Zhang",
        team="quant",
        discord_user="kai#1234",
        description="daily test data for the proposal flow",
    )
    defaults.update(overrides)
    return DatasetProposal(**defaults)


def test_happy_path_opens_pr_with_files() -> None:
    github = FakeGitHub()
    result = propose_dataset(_proposal(), requested_by="team-a", client=github)

    assert result.pr_url == "https://github.com/acme/data/pull/42"
    assert result.branch == "add-dataset/my-dataset-0.1.0"

    call = github.calls[0]
    assert call["base"] == "main"
    assert call["title"] == "feat: add dataset my-dataset/0.1.0"
    files = call["files"]
    assert set(files) == {
        "builders/scripts/my-dataset/0.1.0/config.toml",
        "builders/scripts/my-dataset/0.1.0/builder.py",
    }
    assert "team-a" in call["body"]
    # proposer identity and purpose are surfaced for reviewers
    assert "Kai Zhang" in call["body"]
    assert "quant" in call["body"]
    assert "kai#1234" in call["body"]
    assert "daily test data for the proposal flow" in call["body"]

    # the committed config parses and carries the submitted fields
    raw = tomllib.loads(files["builders/scripts/my-dataset/0.1.0/config.toml"])
    assert raw["name"] == "my-dataset"
    assert raw["schema"] == {"ticker": "str", "price": "float"}


def test_dependency_with_lookback_round_trips() -> None:
    github = FakeGitHub()
    proposal = _proposal(
        dependencies=[
            ProposedDependency(name="mock-dep", version="0.1.0", lookback="5d")
        ]
    )
    propose_dataset(proposal, requested_by="team-a", client=github)

    config = github.calls[0]["files"]["builders/scripts/my-dataset/0.1.0/config.toml"]
    raw = tomllib.loads(config)
    assert raw["dependencies"]["mock-dep"] == {"version": "0.1.0", "lookback": "5d"}


def test_optional_files_included_when_present() -> None:
    github = FakeGitHub()
    proposal = _proposal(
        env_vars=True,
        requirements_txt="pandas>=2.0\n",
        env_template="API_KEY=\n",
    )
    propose_dataset(proposal, requested_by="team-a", client=github)

    call = github.calls[0]
    files = call["files"]
    assert "builders/scripts/my-dataset/0.1.0/requirements.txt" in files
    assert "builders/scripts/my-dataset/0.1.0/.env.template" in files
    # the env checklist reminds reviewers secrets are placed manually
    assert ".env" in call["body"]
    assert "before first build" in call["body"]


def test_env_file_itself_is_never_committed() -> None:
    github = FakeGitHub()
    propose_dataset(
        _proposal(env_vars=True, env_template="API_KEY=\n"),
        requested_by="team-a",
        client=github,
    )
    files = github.calls[0]["files"]
    assert "builders/scripts/my-dataset/0.1.0/.env" not in files


def test_unknown_dependency_rejected() -> None:
    proposal = _proposal(
        dependencies=[ProposedDependency(name="nope", version="9.9.9")]
    )
    with pytest.raises(InvalidProposalError, match="not a known dataset"):
        propose_dataset(proposal, requested_by="t", client=FakeGitHub())


def test_granularity_finer_than_dependency_rejected() -> None:
    proposal = _proposal(
        granularity="1h",
        dependencies=[ProposedDependency(name="mock-dep", version="0.1.0")],
    )
    with pytest.raises(InvalidProposalError, match="finer than dependency"):
        propose_dataset(proposal, requested_by="t", client=FakeGitHub())


def test_start_date_before_dependency_rejected() -> None:
    proposal = _proposal(
        start_date="2020-01-01",
        dependencies=[ProposedDependency(name="mock-dep", version="0.1.0")],
    )
    with pytest.raises(InvalidProposalError, match="start-date"):
        propose_dataset(proposal, requested_by="t", client=FakeGitHub())


def test_existing_dataset_conflicts() -> None:
    proposal = _proposal(name="mock-dep", version="0.1.0", start_date="2021-06-01")
    with pytest.raises(ProposalConflictError, match="already exists"):
        propose_dataset(proposal, requested_by="t", client=FakeGitHub())


def test_existing_branch_conflicts() -> None:
    github = FakeGitHub(error=BranchAlreadyExistsError(422, "Reference already exists"))
    with pytest.raises(StaleProposalConflictError, match="not registered"):
        propose_dataset(_proposal(), requested_by="t", client=github)


def test_stale_branch_conflict_reports_open_pr_url() -> None:
    github = FakeGitHub(
        error=BranchAlreadyExistsError(422, "Reference already exists"),
        open_pr={"number": 11, "html_url": "https://github.com/acme/data/pull/11"},
    )
    with pytest.raises(StaleProposalConflictError) as exc_info:
        propose_dataset(_proposal(), requested_by="t", client=github)
    assert exc_info.value.open_pr_url == "https://github.com/acme/data/pull/11"
    # no confirmation yet -- nothing should have been touched
    assert github.closed_prs == []
    assert github.deleted_branches == []


def test_stale_branch_conflict_without_open_pr_reports_none() -> None:
    github = FakeGitHub(error=BranchAlreadyExistsError(422, "Reference already exists"))
    with pytest.raises(StaleProposalConflictError) as exc_info:
        propose_dataset(_proposal(), requested_by="t", client=github)
    assert exc_info.value.open_pr_url is None


def test_override_closes_open_pr_and_deletes_branch_before_retry() -> None:
    github = FakeGitHub(
        error=BranchAlreadyExistsError(422, "Reference already exists"),
        open_pr={"number": 11, "html_url": "https://github.com/acme/data/pull/11"},
    )
    result = propose_dataset(
        _proposal(), requested_by="t", client=github, override=True
    )
    assert result.pr_url == "https://github.com/acme/data/pull/42"
    assert github.closed_prs == [11]
    assert github.deleted_branches == ["add-dataset/my-dataset-0.1.0"]
    assert len(github.calls) == 1


def test_override_without_open_pr_only_deletes_branch() -> None:
    github = FakeGitHub(error=BranchAlreadyExistsError(422, "Reference already exists"))
    propose_dataset(_proposal(), requested_by="t", client=github, override=True)
    assert github.closed_prs == []
    assert github.deleted_branches == ["add-dataset/my-dataset-0.1.0"]


def test_override_still_conflicting_after_clear_raises_hard_conflict() -> None:
    """a concurrent proposal recreated the branch between clear and retry."""

    class StillConflicting(FakeGitHub):
        def open_pr_with_files(self, *args: Any, **kwargs: Any) -> str:
            raise BranchAlreadyExistsError(422, "Reference already exists")

    github = StillConflicting()
    with pytest.raises(ProposalConflictError, match="still exists after override"):
        propose_dataset(_proposal(), requested_by="t", client=github, override=True)


@pytest.mark.parametrize(
    "bad_name", ["MyDataset", "my dataset", "../escape", "-leading", ""]
)
def test_invalid_names_rejected(bad_name: str) -> None:
    with pytest.raises(InvalidProposalError, match="dataset name"):
        propose_dataset(_proposal(name=bad_name), requested_by="t", client=FakeGitHub())


@pytest.mark.parametrize(
    "field_name", ["author_name", "team", "discord_user", "description"]
)
def test_blank_proposer_fields_rejected(field_name: str) -> None:
    with pytest.raises(InvalidProposalError, match=field_name):
        propose_dataset(
            _proposal(**{field_name: "   "}), requested_by="t", client=FakeGitHub()
        )


def test_invalid_version_rejected() -> None:
    with pytest.raises(InvalidProposalError, match="version"):
        propose_dataset(
            _proposal(version="not-semver"), requested_by="t", client=FakeGitHub()
        )


def test_invalid_schema_type_rejected() -> None:
    with pytest.raises(InvalidProposalError):
        propose_dataset(
            _proposal(schema={"price": "decimal"}),
            requested_by="t",
            client=FakeGitHub(),
        )


def test_unknown_calendar_rejected() -> None:
    with pytest.raises(InvalidProposalError):
        propose_dataset(
            _proposal(calendar="lunar"), requested_by="t", client=FakeGitHub()
        )


def test_builder_syntax_error_rejected() -> None:
    with pytest.raises(InvalidProposalError, match="syntax error"):
        propose_dataset(
            _proposal(builder_script="def build(:\n"),
            requested_by="t",
            client=FakeGitHub(),
        )


def test_builder_without_build_rejected() -> None:
    with pytest.raises(InvalidProposalError, match="build\\(\\)"):
        propose_dataset(
            _proposal(builder_script="def make(a, b):\n    return []\n"),
            requested_by="t",
            client=FakeGitHub(),
        )


def test_builder_wrong_arity_rejected() -> None:
    with pytest.raises(InvalidProposalError, match="two arguments"):
        propose_dataset(
            _proposal(builder_script="def build(only_one):\n    return []\n"),
            requested_by="t",
            client=FakeGitHub(),
        )


def test_default_reviewers_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_REVIEWERS", raising=False)
    github = FakeGitHub()
    propose_dataset(_proposal(), requested_by="t", client=github)
    assert github.calls[0]["reviewers"] == ["Blackgaurd", "Scr4tch587"]


def test_reviewers_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_REVIEWERS", "alice, bob")
    github = FakeGitHub()
    propose_dataset(_proposal(), requested_by="t", client=github)
    assert github.calls[0]["reviewers"] == ["alice", "bob"]


def test_builder_script_is_autofixed_before_commit() -> None:
    """unused imports and formatting are cleaned server-side so the pr passes ci."""
    github = FakeGitHub()
    script = (
        "from datetime import datetime\n"
        "from typing import Any\n\n\n"
        "def build(dependencies, timestamp):\n"
        "    return [{'ticker': 'AAPL', 'price': 1.0}]\n"
    )
    propose_dataset(_proposal(builder_script=script), requested_by="t", client=github)
    committed = github.calls[0]["files"]["builders/scripts/my-dataset/0.1.0/builder.py"]
    assert "from typing import Any" not in committed  # unused import removed
    assert "from datetime import datetime" not in committed
    assert '"AAPL"' in committed  # ruff format normalizes quotes


def test_unfixable_lint_error_rejected() -> None:
    """violations ruff cannot autofix (e.g. undefined name) reject the proposal."""
    script = "def build(dependencies, timestamp):\n    return [undefined_var]\n"
    with pytest.raises(InvalidProposalError, match="fails lint"):
        propose_dataset(
            _proposal(builder_script=script), requested_by="t", client=FakeGitHub()
        )


def test_lint_error_surfaces_ruff_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    """a ruff failure that only writes to stderr still reaches the caller."""

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=2, stdout="", stderr="ruff failed: bad config"
        )

    monkeypatch.setattr(proposals.subprocess, "run", fake_run)
    with pytest.raises(InvalidProposalError, match="ruff failed: bad config"):
        propose_dataset(_proposal(), requested_by="t", client=FakeGitHub())


def test_lint_error_reports_exit_code_when_ruff_is_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """no output on either stream still produces a non-empty diagnostic."""

    def fake_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=[], returncode=101, stdout="", stderr=""
        )

    monkeypatch.setattr(proposals.subprocess, "run", fake_run)
    with pytest.raises(InvalidProposalError, match="exited with code 101"):
        propose_dataset(_proposal(), requested_by="t", client=FakeGitHub())


def test_generated_toml_quotes_awkward_schema_keys() -> None:
    """schema keys that aren't bare toml keys are quoted, not mangled."""
    proposal = _proposal(schema={"has space": "str", "ok_key": "int"})
    raw = tomllib.loads(generate_config_toml(proposal))
    assert raw["schema"] == {"has space": "str", "ok_key": "int"}
