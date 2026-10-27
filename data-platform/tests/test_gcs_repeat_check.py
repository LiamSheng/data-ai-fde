"""Offline checks of the cloud acceptance harness; no ADC or network."""

import hashlib
import json
import runpy
from pathlib import Path

import pytest
from google.api_core.exceptions import Forbidden, PreconditionFailed

CHECK = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/verify_gcs_repeat.py")
)
run_check = CHECK["run_check"]
CheckFailed = CHECK["CheckFailed"]
CHECK_ID = "a" * 32
PREFIX = f"validation/gcs_repeat/{CHECK_ID}/"


class FakeBlob:
    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.generation = None

    def upload_from_string(self, data, **kwargs):
        assert self.name.startswith(PREFIX)
        assert kwargs["if_generation_match"] == 0
        assert kwargs["retry"] is None
        self.client.calls.append(self.name)
        if self.client.fail_audit and self.name.endswith(".json"):
            raise RuntimeError("untrusted error including a credential")
        if self.name in self.client.objects:
            if self.client.forbidden_existing:
                raise Forbidden("permission denied")
            if not self.client.replace_existing:
                raise PreconditionFailed("object already exists")
        self.generation = len(self.client.calls)
        self.client.objects[self.name] = bytes(data)
        self.client.generations[self.name] = str(self.generation)


class FakeBucket:
    def __init__(self, client):
        self.client = client

    def blob(self, name):
        return FakeBlob(self.client, name)


class FakeClient:
    def __init__(
        self, *, forbidden_existing=False, replace_existing=False, fail_audit=False
    ):
        self.objects = {}
        self.generations = {}
        self.calls = []
        self.forbidden_existing = forbidden_existing
        self.replace_existing = replace_existing
        self.fail_audit = fail_audit

    def bucket(self, name):
        assert name == "test-landing-bucket"
        return FakeBucket(self)


def test_two_captures_share_one_payload_and_keep_two_audits(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", "must-not-be-used")
    client = FakeClient()
    report = run_check("test-landing-bucket", CHECK_ID, client=client)
    payload_name = PREFIX + report["payload_ref"]
    audits = [
        json.loads(data)
        for name, data in client.objects.items()
        if name.endswith(".json")
    ]
    assert report["storage_dispositions"] == ["new", "existing"]
    assert report["mock_http_requests"] == 2
    assert len(client.objects) == 3
    assert len(audits) == 2
    assert len(set(report["attempt_ids"])) == 2
    assert len(set(report["poll_cycle_ids"])) == 2
    assert client.calls == [
        payload_name,
        PREFIX + f"audit/capture_attempts/{report['attempt_ids'][0]}.json",
        payload_name,
        PREFIX + f"audit/capture_attempts/{report['attempt_ids'][1]}.json",
    ]
    assert report["created_payload_generation"] == client.generations[payload_name]
    assert (
        hashlib.sha256(client.objects[payload_name]).hexdigest()
        == report["payload_sha256"]
    )
    assert len(client.objects[payload_name]) == report["payload_bytes"]
    for audit in audits:
        assert audit["outcome"] == "success"
        assert audit["http_status"] == 200
        assert audit["payload_ref"] == report["payload_ref"]
        assert audit["payload_sha256"] == report["payload_sha256"]
        assert "example.invalid" not in json.dumps(audit)
        assert CHECK["SYNTHETIC_KEY"] not in json.dumps(audit)
    assert report["independent_object_verification"] == "pending"
    assert not (tmp_path / "data").exists()
    assert CHECK["os"].environ["GTFS_RT_API_KEY"] == "must-not-be-used"


@pytest.mark.parametrize("bad_id", ["", "../production", "A" * 32, "a" * 31])
def test_bad_check_id_is_rejected_before_writes(bad_id):
    client = FakeClient()
    with pytest.raises(CheckFailed, match="invalid_check_id"):
        run_check("test-landing-bucket", bad_id, client=client)
    assert client.calls == []


def test_forbidden_duplicate_remains_failure_not_existing():
    client = FakeClient(forbidden_existing=True)
    with pytest.raises(CheckFailed, match="capture_2_failed"):
        run_check("test-landing-bucket", CHECK_ID, client=client)
    records = [
        json.loads(data)
        for name, data in client.objects.items()
        if name.endswith(".json")
    ]
    assert [record["outcome"] for record in records] == ["success", "storage_error"]


def test_unconditional_duplicate_write_is_detected():
    client = FakeClient(replace_existing=True)
    with pytest.raises(CheckFailed, match="unexpected_storage_dispositions"):
        run_check("test-landing-bucket", CHECK_ID, client=client)


def test_audit_failure_cannot_pass_acceptance(capsys):
    client = FakeClient(fail_audit=True)
    with pytest.raises(CheckFailed, match="capture_1_failed"):
        run_check("test-landing-bucket", CHECK_ID, client=client)
    output = capsys.readouterr()
    assert "audit_unavailable" in output.err
    assert "untrusted error" not in output.out + output.err
    assert not any(name.endswith(".json") for name in client.objects)


def test_main_redacts_unknown_runtime_errors(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise RuntimeError("sensitive-secret-from-an-sdk-error")

    monkeypatch.setattr(CHECK["gcs_sdk"], "Client", fail)
    assert (
        CHECK["main"](["--bucket", "test-landing-bucket", "--check-id", CHECK_ID]) == 1
    )
    output = capsys.readouterr()
    assert "verification_failed=runtime_or_storage" in output.err
    assert "sensitive-secret" not in output.out + output.err
