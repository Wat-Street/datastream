import core.api.routes as routes
import pytest
from core.github.client import GitHubError
from core.service.proposals import (
    InvalidProposalError,
    ProposalConflictError,
    ProposalResult,
    StaleProposalConflictError,
)
from fastapi.testclient import TestClient
from main import app

client: TestClient = TestClient(app)

PAYLOAD = {
    "name": "my-dataset",
    "version": "0.1.0",
    "calendar": "everyday",
    "granularity": "1d",
    "start_date": "2022-01-01",
    "schema": {"price": "float"},
    "builder_script": "def build(deps, ts):\n    return []\n",
    "author_name": "Kai Zhang",
    "team": "quant",
    "discord_user": "kai#1234",
    "description": "test data",
}


def _propose_stub(result=None, error=None):
    def stub(proposal, requested_by, client=None, override=False):
        if error is not None:
            raise error
        return result

    return stub


def test_propose_returns_pr_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(
            result=ProposalResult(
                pr_url="https://github.com/acme/data/pull/42",
                branch="add-dataset/my-dataset-0.1.0",
            )
        ),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 200
    body = res.json()
    assert body["pr_url"] == "https://github.com/acme/data/pull/42"
    assert body["branch"] == "add-dataset/my-dataset-0.1.0"
    assert body["dataset_name"] == "my-dataset"


def test_invalid_proposal_maps_to_400(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(error=InvalidProposalError("bad schema")),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 400
    assert "bad schema" in res.json()["detail"]


def test_conflict_maps_to_409(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(error=ProposalConflictError("already exists")),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 409
    body = res.json()
    assert body["conflict_type"] == "dataset_exists"
    assert "already exists" in body["detail"]


def test_stale_branch_conflict_maps_to_409_with_open_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(
            error=StaleProposalConflictError(
                "not registered",
                branch="add-dataset/my-dataset-0.1.0",
                open_pr_url="https://github.com/acme/data/pull/11",
            )
        ),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 409
    body = res.json()
    assert body["conflict_type"] == "stale_branch"
    assert body["open_pr_url"] == "https://github.com/acme/data/pull/11"


def test_stale_branch_conflict_without_open_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(
            error=StaleProposalConflictError(
                "not registered",
                branch="add-dataset/my-dataset-0.1.0",
                open_pr_url=None,
            )
        ),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 409
    assert res.json()["open_pr_url"] is None


def test_override_flag_passed_through(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def stub(proposal, requested_by, client=None, override=False):
        captured["override"] = override
        return ProposalResult(
            pr_url="https://github.com/acme/data/pull/42",
            branch="add-dataset/my-dataset-0.1.0",
        )

    monkeypatch.setattr(routes, "propose_dataset", stub)
    res = client.post("/api/v1/datasets", json={**PAYLOAD, "override": True})
    assert res.status_code == 200
    assert captured["override"] is True


def test_github_failure_maps_to_502(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        routes,
        "propose_dataset",
        _propose_stub(error=GitHubError(500, "boom")),
    )
    res = client.post("/api/v1/datasets", json=PAYLOAD)
    assert res.status_code == 502
    assert "github error" in res.json()["detail"]


def test_missing_fields_rejected_by_validation() -> None:
    res = client.post("/api/v1/datasets", json={"name": "x"})
    assert res.status_code == 422
