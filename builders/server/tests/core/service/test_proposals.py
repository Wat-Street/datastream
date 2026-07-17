import tomllib
from pathlib import Path
from typing import Any

import core.runtime.registry as registry
import pytest
from core.github.client import BranchAlreadyExistsError
from core.service.proposals import (
    DatasetProposal,
    InvalidProposalError,
    ProposalConflictError,
    ProposedDependency,
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
    """stands in for GitHubClient; records the pr call or raises."""

    def __init__(self, error: Exception | None = None):
        self.error = error
        self.calls: list[dict[str, Any]] = []

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
            raise self.error
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

    config = github.calls[0]["files"][
        "builders/scripts/my-dataset/0.1.0/config.toml"
    ]
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
    with pytest.raises(ProposalConflictError, match="already open"):
        propose_dataset(_proposal(), requested_by="t", client=github)


@pytest.mark.parametrize(
    "bad_name", ["MyDataset", "my dataset", "../escape", "-leading", ""]
)
def test_invalid_names_rejected(bad_name: str) -> None:
    with pytest.raises(InvalidProposalError, match="dataset name"):
        propose_dataset(
            _proposal(name=bad_name), requested_by="t", client=FakeGitHub()
        )


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
    committed = github.calls[0]["files"][
        "builders/scripts/my-dataset/0.1.0/builder.py"
    ]
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


def test_generated_toml_quotes_awkward_schema_keys() -> None:
    """schema keys that aren't bare toml keys are quoted, not mangled."""
    proposal = _proposal(schema={"has space": "str", "ok_key": "int"})
    raw = tomllib.loads(generate_config_toml(proposal))
    assert raw["schema"] == {"has space": "str", "ok_key": "int"}
