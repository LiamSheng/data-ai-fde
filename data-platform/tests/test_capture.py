"""Integration tests across HTTP, raw storage, parsing, and audit records."""

import hashlib
import json
from pathlib import Path

import httpx
import pytest

from transit_realtime_pipeline import capture

REPO_ROOT = Path(__file__).resolve().parents[1]

# This is the real snapshot captured earlier, not a generated success payload.
CAPTURED_SHA = "0db542eeb80638f082438aedaeb50ba8a6099d3d0c60b8c36a78b2bab5f91e4b"
CAPTURED_REF = f"vehicle_positions/0d/{CAPTURED_SHA}.pb"
CAPTURED_PATH = REPO_ROOT / "data/raw/gtfs_rt" / CAPTURED_REF
CAPTURED_BYTES = 90336
CAPTURED_ENTITIES = 1007
CAPTURED_TIMESTAMP = "2026-09-23T21:39:23Z"

# Only a test credential. The real API key is never used by these tests.
FAKE_KEY = "test-secret-value"
FAKE_URL = "https://example.test/vehicle-positions"


@pytest.fixture
def captured_payload() -> bytes:
    """Load and identify the locally saved real snapshot."""
    if not CAPTURED_PATH.is_file():
        pytest.skip("local captured .pb is unavailable")

    payload = CAPTURED_PATH.read_bytes()
    assert len(payload) == CAPTURED_BYTES
    assert hashlib.sha256(payload).hexdigest() == CAPTURED_SHA
    return payload


@pytest.fixture
def isolated_run(tmp_path, monkeypatch):
    """Give each test its own data directory and fake source configuration."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", FAKE_KEY)
    monkeypatch.setenv("GTFS_RT_VEHICLE_POSITIONS_URL", FAKE_URL)
    return tmp_path


@pytest.fixture
def install_transport(monkeypatch):
    """Replace only the HTTP boundary; capture.main() still runs normally."""
    real_client = httpx.Client

    def install(handler):
        def client_with_mock_transport(*args, **kwargs):
            return real_client(
                *args,
                transport=httpx.MockTransport(handler),
                **kwargs,
            )

        monkeypatch.setattr(capture.httpx, "Client", client_with_mock_transport)

    return install


def install_response(install_transport, status: int, body: bytes = b"") -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.params["apikey"] == FAKE_KEY
        return httpx.Response(status, content=body)

    install_transport(respond)


def record_paths(root: Path) -> list[Path]:
    return sorted((root / "data/ops/capture_attempts").glob("*.json"))


def one_record(root: Path) -> dict:
    paths = record_paths(root)
    assert len(paths) == 1, "one request must produce one audit record"

    record = json.loads(paths[0].read_text(encoding="utf-8"))
    assert paths[0].stem == record["attempt_id"]
    assert record["source_id"] == "primary"
    assert record["feed_type"] == "vehicle_positions"
    assert "started_at_utc" in record
    assert "completed_at_utc" in record
    return record


def assert_no_secret(record: dict, capsys) -> None:
    output = capsys.readouterr()
    visible_text = json.dumps(record) + output.out + output.err

    assert FAKE_KEY not in visible_text
    assert FAKE_URL not in visible_text


def test_real_snapshot_produces_payload_and_success_record(
    isolated_run, install_transport, captured_payload, capsys
) -> None:
    install_response(install_transport, 200, captured_payload)

    assert capture.main() == 0

    record = one_record(isolated_run)
    assert record["outcome"] == "success"
    assert record["http_status"] == 200
    assert record["payload_ref"] == CAPTURED_REF
    assert record["payload_sha256"] == CAPTURED_SHA
    assert record["payload_bytes"] == CAPTURED_BYTES
    assert record["feed_timestamp"] == CAPTURED_TIMESTAMP
    assert record["incrementality"] == "FULL_DATASET"
    assert record["entity_count"] == CAPTURED_ENTITIES
    assert isinstance(record["feed_age_seconds"], (int, float))

    saved = isolated_run / "data/raw/gtfs_rt" / record["payload_ref"]
    assert saved.read_bytes() == captured_payload
    assert_no_secret(record, capsys)


def test_repeated_snapshot_creates_two_attempts_but_one_payload(
    isolated_run, install_transport, captured_payload, capsys
) -> None:
    install_response(install_transport, 200, captured_payload)

    assert capture.main() == 0
    assert capture.main() == 0

    paths = record_paths(isolated_run)
    assert len(paths) == 2
    records = [json.loads(path.read_text(encoding="utf-8")) for path in paths]

    assert records[0]["attempt_id"] != records[1]["attempt_id"]
    assert {record["payload_ref"] for record in records} == {CAPTURED_REF}
    assert {record["payload_sha256"] for record in records} == {CAPTURED_SHA}

    raw_files = list((isolated_run / "data/raw/gtfs_rt").rglob("*.pb"))
    assert len(raw_files) == 1

    output = capsys.readouterr()
    assert "storage=new" in output.out
    assert "storage=reused" in output.out
    assert FAKE_KEY not in output.out + output.err


def test_401_creates_authentication_failure_record(
    isolated_run, install_transport, capsys
) -> None:
    install_response(install_transport, 401)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "http_error"
    assert record["http_status"] == 401
    assert record["error_category"] == "authentication"
    assert "payload_ref" not in record
    assert "feed_timestamp" not in record
    assert list((isolated_run / "data/raw/gtfs_rt").rglob("*.pb")) == []
    assert_no_secret(record, capsys)


def test_500_creates_server_failure_record(
    isolated_run, install_transport, capsys
) -> None:
    install_response(install_transport, 500)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "http_error"
    assert record["http_status"] == 500
    assert record["error_category"] == "http_server"
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_timeout_creates_failure_record_without_exception_text(
    isolated_run, install_transport, capsys
) -> None:
    def time_out(request: httpx.Request) -> httpx.Response:
        # Deliberately put the fake key in the exception text.
        raise httpx.ReadTimeout(
            f"request timed out: {request.url}",
            request=request,
        )

    install_transport(time_out)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "timeout"
    assert record["error_category"] == "timeout"
    assert "http_status" not in record
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_transport_error_creates_failure_record_without_url(
    isolated_run, install_transport, capsys
) -> None:
    def connection_fails(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(
            f"cannot connect to {request.url}",
            request=request,
        )

    install_transport(connection_fails)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "transport_error"
    assert record["error_category"] == "transport"
    assert "http_status" not in record
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)


def test_invalid_protobuf_preserves_raw_bytes_and_audits_failure(
    isolated_run, install_transport, capsys
) -> None:
    invalid_bytes = b"\xff"
    install_response(install_transport, 200, invalid_bytes)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "invalid_protobuf"
    assert record["error_category"] == "invalid_protobuf"
    assert record["http_status"] == 200
    assert record["payload_bytes"] == len(invalid_bytes)
    assert record["payload_sha256"] == hashlib.sha256(invalid_bytes).hexdigest()
    assert "feed_timestamp" not in record

    # HTTP delivered bytes, so keep them for diagnosis even though parsing failed.
    saved = isolated_run / "data/raw/gtfs_rt" / record["payload_ref"]
    assert saved.read_bytes() == invalid_bytes
    assert_no_secret(record, capsys)


def test_storage_failure_is_audited_after_http_200(
    isolated_run, install_transport, captured_payload, monkeypatch, capsys
) -> None:
    install_response(install_transport, 200, captured_payload)

    def fail_to_save(payload: bytes, storage_root: Path, feed_type: str):
        assert feed_type == "vehicle_positions"
        raise OSError(f"cannot save; diagnostic contained {FAKE_KEY}")

    monkeypatch.setattr(capture, "save_raw_payload", fail_to_save)

    assert capture.main() == 1

    record = one_record(isolated_run)
    assert record["outcome"] == "storage_error"
    assert record["error_category"] == "storage"
    assert record["http_status"] == 200
    assert "payload_ref" not in record
    assert_no_secret(record, capsys)
