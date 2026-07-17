"""Dataset proposal service: validate a submission and open a GitHub PR.

Proposed datasets are never written to the server's scripts directory — code
review stays the gate for anything that will execute on the server. This
module turns a submission into the exact files that will land in
builders/scripts/<name>/<version>/, re-validates the generated config bytes
with the same checks the server runs at startup, and opens the PR via the
github client.
"""

import ast
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import structlog

from core.github import BranchAlreadyExistsError, GitHubClient
from core.runtime import registry
from core.runtime.config import DatasetConfig, normalize_config, validate_config
from core.utils.semver import SemVer

logger = structlog.get_logger()

# dataset names become directory names, branch names, and toml keys
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_BARE_TOML_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")

SCRIPTS_PREFIX = "builders/scripts"

# keep in sync with [tool.ruff.lint] select in the repo-root pyproject.toml:
# proposal prs must pass the same ci lint that runs over builders/scripts/**
RUFF_SELECT = "B,E,F,I,PIE,SIM,T20,UP"
RUFF_TIMEOUT_SECONDS = 30

# github logins asked to review every proposal pr (comma-separated env override)
DEFAULT_REVIEWERS = "Blackgaurd,Scr4tch587"


class InvalidProposalError(ValueError):
    """Submission failed validation; safe to show the message to the caller."""


class ProposalConflictError(Exception):
    """The dataset or its proposal branch already exists."""


@dataclass(frozen=True)
class ProposedDependency:
    name: str
    version: str
    lookback: str | None = None


@dataclass(frozen=True)
class DatasetProposal:
    name: str
    version: str
    calendar: str
    granularity: str
    start_date: str
    schema: dict[str, str]
    builder_script: str
    # who is proposing and why -- surfaced in the pr body for reviewers
    author_name: str
    team: str
    discord_user: str
    description: str
    dependencies: list[ProposedDependency] = field(default_factory=list)
    env_vars: bool = False
    requirements_txt: str | None = None
    env_template: str | None = None


@dataclass(frozen=True)
class ProposalResult:
    pr_url: str
    branch: str


class PullRequestOpener(Protocol):
    """The slice of GitHubClient the proposal service needs (test-fakeable)."""

    def open_pr_with_files(
        self,
        branch: str,
        base: str,
        title: str,
        body: str,
        commit_message: str,
        files: dict[str, str],
        reviewers: list[str] | None = None,
    ) -> str: ...


def _toml_str(value: str) -> str:
    """quote a string as a toml basic string

    (json escaping is a valid subset).
    """
    return json.dumps(value)


def _toml_key(key: str) -> str:
    return key if _BARE_TOML_KEY_RE.fullmatch(key) else json.dumps(key)


def generate_config_toml(proposal: DatasetProposal) -> str:
    """Render the canonical config.toml for a proposal.

    The returned text is exactly what gets committed; callers re-parse and
    re-validate these bytes so the PR can never contain a config that fails
    server startup.
    """
    lines = [
        f"name = {_toml_str(proposal.name)}",
        f"version = {_toml_str(proposal.version)}",
        'builder = "builder.py"',
        f"calendar = {_toml_str(proposal.calendar)}",
        f"granularity = {_toml_str(proposal.granularity)}",
        f"start-date = {_toml_str(proposal.start_date)}",
    ]
    if proposal.env_vars:
        lines.append("env-vars = true")

    lines += ["", "[schema]"]
    lines += [
        f"{_toml_key(key)} = {_toml_str(type_)}"
        for key, type_ in proposal.schema.items()
    ]

    if proposal.dependencies:
        lines += ["", "[dependencies]"]
        for dep in proposal.dependencies:
            if dep.lookback:
                lines.append(
                    f"{_toml_key(dep.name)} = {{ version = {_toml_str(dep.version)},"
                    f" lookback = {_toml_str(dep.lookback)} }}"
                )
            else:
                lines.append(f"{_toml_key(dep.name)} = {_toml_str(dep.version)}")

    return "\n".join(lines) + "\n"


def _validate_builder_script(script: str) -> None:
    """Same convention the ci gate enforces:

    parseable, top-level build(dependencies, timestamp).
    """
    try:
        tree = ast.parse(script)
    except SyntaxError as e:
        raise InvalidProposalError(f"builder script has a syntax error: {e}") from e

    build_fns = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build"
    ]
    if not build_fns:
        raise InvalidProposalError(
            "builder script must define a top-level build() function"
        )
    args = build_fns[0].args
    positional = args.posonlyargs + args.args
    if len(positional) != 2:
        raise InvalidProposalError(
            "build() must take exactly two arguments (dependencies, timestamp), "
            f"got {[a.arg for a in positional]}"
        )


def _lint_builder_script(script: str) -> str:
    """Autofix + format the script with ruff so the proposal pr passes repo ci.

    Returns the cleaned script. Raises InvalidProposalError when violations
    remain after autofix (the message includes ruff's output).
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "builder.py"
        path.write_text(script)
        common = [sys.executable, "-m", "ruff"]
        check = subprocess.run(
            [*common, "check", "--isolated", "--select", RUFF_SELECT, "--fix", str(path)],
            capture_output=True,
            text=True,
            timeout=RUFF_TIMEOUT_SECONDS,
        )
        if check.returncode != 0:
            raise InvalidProposalError(
                f"builder script fails lint:\n{check.stdout.strip()}"
            )
        fmt = subprocess.run(
            [*common, "format", "--isolated", str(path)],
            capture_output=True,
            text=True,
            timeout=RUFF_TIMEOUT_SECONDS,
        )
        if fmt.returncode != 0:
            raise InvalidProposalError(
                f"builder script fails formatting:\n{fmt.stderr.strip()}"
            )
        return path.read_text()


def _validate_against_registry(cfg: DatasetConfig) -> None:
    """Cross-checks against currently registered datasets.

    A new dataset is a leaf no existing config references, so it cannot
    introduce a cycle — only dep existence, granularity, and start-date
    ordering need checking (mirrors registry startup validation).
    """
    for dep_name, dep_info in cfg.dependencies.items():
        try:
            dep_cfg = registry.get_config(dep_name, dep_info.version)
        except ValueError as e:
            raise InvalidProposalError(
                f"dependency {dep_name}/{dep_info.version} is not a known dataset"
            ) from e
        if cfg.granularity < dep_cfg.granularity:
            raise InvalidProposalError(
                f"granularity is finer than dependency {dep_name}/{dep_info.version}"
            )
        if cfg.start_date < dep_cfg.start_date:
            raise InvalidProposalError(
                f"start-date is before dependency {dep_name}/{dep_info.version}'s "
                f"start-date ({dep_cfg.start_date.date()})"
            )


def _build_pr_body(
    proposal: DatasetProposal, requested_by: str, dataset_dir: str
) -> str:
    schema_lines = "\n".join(
        f"- `{key}`: `{type_}`" for key, type_ in proposal.schema.items()
    )
    if proposal.dependencies:
        dep_lines = "\n".join(
            f"- `{dep.name}/{dep.version}`"
            + (f" (lookback `{dep.lookback}`)" if dep.lookback else "")
            for dep in proposal.dependencies
        )
    else:
        dep_lines = "- none (root dataset)"

    body = f"""## Dataset proposal: `{proposal.name}/{proposal.version}`

{proposal.description.strip()}

**Proposed by:** {proposal.author_name.strip()} · team **{proposal.team.strip()}** \
· discord `{proposal.discord_user.strip()}` · api key label `{requested_by}`

| field | value |
|-------|-------|
| calendar | `{proposal.calendar}` |
| granularity | `{proposal.granularity}` |
| start-date | `{proposal.start_date}` |
| env-vars | `{str(proposal.env_vars).lower()}` |

**Schema**
{schema_lines}

**Dependencies**
{dep_lines}

### Review checklist

- [ ] builder logic reviewed — this code will run on the datastream server
- [ ] schema and dependencies make sense for the data
"""
    if proposal.requirements_txt:
        body += "- [ ] `requirements.txt` packages reviewed\n"
    if proposal.env_vars:
        body += (
            f"- [ ] **before first build**: place the real `.env` at "
            f"`{dataset_dir}/.env` on the server "
            f"(only `.env.template` is committed; see it for required vars)\n"
        )
    return body


def propose_dataset(
    proposal: DatasetProposal,
    requested_by: str,
    client: PullRequestOpener | None = None,
) -> ProposalResult:
    """Validate a proposal and open a PR adding the dataset directory.

    Raises InvalidProposalError (bad submission), ProposalConflictError
    (dataset or proposal branch already exists), or GitHubError (github
    unreachable / misconfigured).
    """
    if not _NAME_RE.fullmatch(proposal.name):
        raise InvalidProposalError(
            "dataset name must be lowercase alphanumeric with '-' or '_' "
            "(it becomes a directory and branch name)"
        )
    for field_name, value in (
        ("author_name", proposal.author_name),
        ("team", proposal.team),
        ("discord_user", proposal.discord_user),
        ("description", proposal.description),
    ):
        if not value.strip():
            raise InvalidProposalError(f"{field_name} must not be empty")
    try:
        version = SemVer.parse(proposal.version)
    except ValueError as e:
        raise InvalidProposalError(f"invalid version: {e}") from e

    try:
        registry.get_config(proposal.name, version)
    except ValueError:
        pass
    else:
        raise ProposalConflictError(
            f"dataset {proposal.name}/{proposal.version} already exists"
        )

    # validate the exact bytes that will be committed, with the same code
    # paths the server runs at startup
    config_toml = generate_config_toml(proposal)
    try:
        raw = tomllib.loads(config_toml)
        validate_config(raw, proposal.name, version)
        normalize_config(raw)
        cfg = DatasetConfig.from_raw(raw)
    except ValueError as e:
        raise InvalidProposalError(str(e)) from e

    _validate_against_registry(cfg)
    _validate_builder_script(proposal.builder_script)
    # commit the linted/formatted script so the pr passes repo ci
    builder_script = _lint_builder_script(proposal.builder_script)

    dataset_dir = f"{SCRIPTS_PREFIX}/{proposal.name}/{proposal.version}"
    files = {
        f"{dataset_dir}/config.toml": config_toml,
        f"{dataset_dir}/builder.py": _ensure_trailing_newline(builder_script),
    }
    if proposal.requirements_txt and proposal.requirements_txt.strip():
        files[f"{dataset_dir}/requirements.txt"] = _ensure_trailing_newline(
            proposal.requirements_txt
        )
    if proposal.env_template and proposal.env_template.strip():
        files[f"{dataset_dir}/.env.template"] = _ensure_trailing_newline(
            proposal.env_template
        )

    branch = f"add-dataset/{proposal.name}-{proposal.version}"
    title = f"feat: add dataset {proposal.name}/{proposal.version}"
    body = _build_pr_body(proposal, requested_by, dataset_dir)

    github: PullRequestOpener = (
        client if client is not None else GitHubClient.from_env()
    )
    reviewers = [
        login.strip()
        for login in os.environ.get("GITHUB_REVIEWERS", DEFAULT_REVIEWERS).split(",")
        if login.strip()
    ]
    try:
        pr_url = github.open_pr_with_files(
            branch=branch,
            base="main",
            title=title,
            body=body,
            commit_message=title,
            files=files,
            reviewers=reviewers,
        )
    except BranchAlreadyExistsError as e:
        raise ProposalConflictError(
            f"a proposal for {proposal.name}/{proposal.version} is already open "
            f"(branch '{branch}' exists)"
        ) from e

    logger.info(
        "dataset proposal submitted",
        dataset=proposal.name,
        version=proposal.version,
        requested_by=requested_by,
        pr_url=pr_url,
    )
    return ProposalResult(pr_url=pr_url, branch=branch)


def _ensure_trailing_newline(text: str) -> str:
    return text if text.endswith("\n") else text + "\n"
