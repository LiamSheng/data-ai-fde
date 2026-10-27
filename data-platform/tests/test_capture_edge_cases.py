"""Edge cases at the capture and audit boundary."""

import hashlib
import json
from pathlib import Path

import httpx
import pytest
from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline import capture

REPO_ROOT = Path(__file__).resolve().parents[1]
CAPTURED_SHA = "0db542eeb80638f082438aedaeb50ba8a6099d3d0c60b8c36a78b2bab5f91e4b"
CAPTURED_PATH = (
    REPO_ROOT / "data/raw/gtfs_rt/vehicle_positions/0d" / f"{CAPTURED_SHA}.pb"
)
FAKE_KEY = "test-secret-value"
FAKE_URL = "https://example.test/vehicle-positions"


@pytest.fixture
def isolated_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", FAKE_KEY)
    monkeypatch.setenv("GTFS_RT_VEHICLE_POSITIONS_URL", FAKE_URL)
    return tmp_path


@pytest.fixture
def captured_feed():
    if not CAPTURED_PATH.is_file():
        pytest.skip("local captured .pb is unavailable")

    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(CAPTURED_PATH.read_bytes())
    assert message.IsInitialized()
    return message


def install_response(monkeypatch, status: int, body: bytes = b"") -> None:
    real_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        def respond(request: httpx.Request) -> httpx.Response:
            assert request.url.params["apikey"] == FAKE_KEY
            return httpx.Response(status, content=body)

        return real_client(
            *args,
            transport=httpx.MockTransport(respond),
            **kwargs,
        )

    monkeypatch.setattr(capture.httpx, "Client", client_with_mock_transport)


def one_record(root: Path) -> dict:
    paths = list((root / "data/ops/capture_attempts").glob("*.json"))
    assert len(paths) == 1

    record = json.loads(paths[0].read_text(encoding="utf-8"))
    assert paths[0].stem == record["attempt_id"]
    return record


def assert_no_secret(record: dict, capsys) -> None:
    output = capsys.readouterr()
    visible = json.dumps(record) + output.out + output.err
    assert FAKE_KEY not in visible
    assert FAKE_URL not in visible


def test_invalid_configuration_is_audited_without_http(
    isolated_run, monkeypatch, capsys
) -> None:
    # A key embedded in a URL is rejected; no request is made.
    monkeypatch.setenv(
        "GTFS_RT_VEHICLE_POSITIONS_URL",
        f"{FAKE_URL}?apikey={FAKE_KEY}",
    )

    assert capture.main() == 2

    record = one_record(isolated_run)
    assert record["outcome"] == "config_error"
    assert record["error_category"] == "configuration"
    assert "http_status" not in record
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_empty_http_body_is_audited_as_invalid_payload(
    isolated_run, monkeypatch, capsys
) -> None:
    install_response(monkeypatch, 200, b"")

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "invalid_protobuf"
    assert record["error_category"] == "invalid_protobuf"
    assert record["http_status"] == 200
    assert record["payload_bytes"] == 0
    assert record["payload_sha256"] == hashlib.sha256(b"").hexdigest()

    saved = isolated_run / "data/raw/gtfs_rt" / record["payload_ref"]
    assert saved.is_file()
    assert saved.read_bytes() == b""
    assert_no_secret(record, capsys)


def test_valid_feed_without_timestamp_has_distinct_audit_outcome(
    isolated_run, captured_feed, monkeypatch, capsys
) -> None:
    # Controlled mutation of the real saved feed, not an invented success.
    captured_feed.header.ClearField("timestamp")
    payload = captured_feed.SerializeToString()
    install_response(monkeypatch, 200, payload)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "feed_metadata_error"
    assert record["error_category"] == "feed_metadata"
    assert record["http_status"] == 200
    assert record["entity_count"] == 1007
    assert "feed_timestamp" not in record

    saved = isolated_run / "data/raw/gtfs_rt" / record["payload_ref"]
    assert saved.read_bytes() == payload
    assert_no_secret(record, capsys)


def test_valid_feed_with_zero_entities_is_still_a_successful_capture(
    isolated_run, captured_feed, monkeypatch, capsys
) -> None:
    # Keep the real header but remove entities to test the empty-feed boundary.
    captured_feed.ClearField("entity")
    install_response(monkeypatch, 200, captured_feed.SerializeToString())

    assert capture.main() == 0

    record = one_record(isolated_run)
    assert record["outcome"] == "success"
    assert record["entity_count"] == 0
    assert record["payload_bytes"] > 0
    assert_no_secret(record, capsys)


def test_unfollowed_redirect_keeps_status_and_uses_other_category(
    isolated_run, monkeypatch, capsys
) -> None:
    install_response(monkeypatch, 302)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "http_error"
    assert record["http_status"] == 302
    assert record["error_category"] == "other"
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_unexpected_exception_uses_safe_fallback_category(
    isolated_run, monkeypatch, capsys
) -> None:
    install_response(monkeypatch, 200, b"received bytes")

    def unexpected_failure(payload: bytes, storage_root: Path, feed_type: str):
        assert feed_type == "vehicle_positions"
        raise RuntimeError(f"internal diagnostic contained {FAKE_KEY}")

    monkeypatch.setattr(capture, "save_raw_payload", unexpected_failure)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "other_error"
    assert record["error_category"] == "other"
    assert record["http_status"] == 200
    assert record["payload_bytes"] == len(b"received bytes")
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_unwritable_audit_sink_returns_safe_error_without_claiming_record(
    isolated_run, monkeypatch, capsys
) -> None:
    install_response(monkeypatch, 401)

    def cannot_write(attempt, record_root):
        raise OSError(f"audit write failed with {FAKE_KEY}")

    monkeypatch.setattr(capture, "write_attempt_record", cannot_write)

    assert capture.main() == 3

    paths = list((isolated_run / "data/ops/capture_attempts").glob("*.json"))
    assert paths == []

    output = capsys.readouterr()
    assert "audit_unavailable" in output.err
    assert FAKE_KEY not in output.out + output.err
