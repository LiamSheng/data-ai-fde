"""Test GCS persistence with fake storage and mocked HTTP only."""

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage as gcs_sdk
from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline import capture, poller
from transit_realtime_pipeline import storage as storage_module
from transit_realtime_pipeline.config import FEED_TYPES, FEED_URL_ENV_VARS
from transit_realtime_pipeline.storage import GCSStorage

BUCKET = "test-landing-bucket"
TEST_API_KEY = "test-key-not-a-real-credential"
FEED_TIMESTAMP = int(datetime(2026, 9, 29, tzinfo=UTC).timestamp())


class FakeBlob:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def upload_from_string(
        self,
        data,
        *,
        content_type,
        if_generation_match,
        timeout,
        retry,
    ):
        # Verify that our adapter supplies the required create-only policy.
        assert if_generation_match == 0
        assert timeout > 0
        assert retry is None

        self.client.calls.append((self.name, content_type))

        is_payload = self.name.endswith(".pb")
        is_audit = self.name.startswith("audit/capture_attempts/")

        if is_payload:
            should_fail = self.client.fail_payload or (
                self.client.fail_payload_prefix is not None
                and self.name.startswith(self.client.fail_payload_prefix)
            )
            if should_fail:
                raise RuntimeError(f"untrusted payload error: {TEST_API_KEY}")

        if is_audit:
            record = json.loads(data)
            should_fail = self.client.fail_audit or (
                self.client.fail_audit_feed is not None
                and record["feed_type"] == self.client.fail_audit_feed
            )
            if should_fail:
                raise RuntimeError(f"untrusted audit error: {TEST_API_KEY}")

        if self.name in self.client.objects:
            raise PreconditionFailed("object already exists")

        self.client.objects[self.name] = bytes(data)

    def download_as_bytes(self):
        raise AssertionError("existing objects must not be downloaded")


class FakeBucket:
    def __init__(self, client):
        self.client = client

    def blob(self, name):
        return FakeBlob(self.client, name)


class FakeClient:
    def __init__(
        self,
        *,
        fail_payload=False,
        fail_audit=False,
        fail_payload_prefix=None,
        fail_audit_feed=None,
    ):
        self.objects = {}
        self.calls = []
        self.bucket_names = []
        self.fail_payload = fail_payload
        self.fail_audit = fail_audit
        self.fail_payload_prefix = fail_payload_prefix
        self.fail_audit_feed = fail_audit_feed

    def bucket(self, name):
        assert name == BUCKET
        self.bucket_names.append(name)
        return FakeBucket(self)


def make_feed(feed_type="vehicle_positions"):
    """Build deterministic bytes to test workflow behavior, not source quality."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = FEED_TIMESTAMP

    entity = feed.entity.add()
    entity.id = "entity-1"

    if feed_type == "vehicle_positions":
        entity.vehicle.vehicle.id = "vehicle-1"
        entity.vehicle.position.latitude = 49.0
        entity.vehicle.position.longitude = -123.0
    elif feed_type == "trip_updates":
        entity.trip_update.trip.trip_id = "trip-1"
    elif feed_type == "service_alerts":
        translation = entity.alert.header_text.translation.add()
        translation.text = "Test service alert"
    else:
        raise AssertionError("unknown test feed")

    assert feed.IsInitialized()
    return feed.SerializeToString()


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch, tmp_path):
    # Each test gets an isolated working directory and fake source configuration.
    # No test reads a real API key or writes runtime data into the repository.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", TEST_API_KEY)
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "local")
    monkeypatch.delenv("GTFS_RT_GCS_BUCKET", raising=False)

    for feed_type, variable in FEED_URL_ENV_VARS.items():
        monkeypatch.setenv(variable, f"https://example.invalid/{feed_type}")

    return tmp_path


def install_http(monkeypatch, handler):
    real_client = httpx.Client
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return real_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(capture.httpx, "Client", client_factory)


def configure_gcs(monkeypatch, client):
    """Exercise the real backend-selection function without real credentials."""
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "gcs")
    monkeypatch.setenv("GTFS_RT_GCS_BUCKET", BUCKET)
    monkeypatch.setattr(gcs_sdk, "Client", lambda: client)


def attempt_records(client):
    return [
        json.loads(contents)
        for name, contents in client.objects.items()
        if name.startswith("audit/capture_attempts/")
    ]


def payload_objects(client):
    return {
        name: contents
        for name, contents in client.objects.items()
        if name.endswith(".pb")
    }


def assert_safe_output(client, output):
    visible = output.out + output.err

    assert TEST_API_KEY not in visible
    assert "example.invalid" not in visible

    for contents in client.objects.values():
        assert TEST_API_KEY.encode() not in contents


def test_new_payload_is_created_conditionally():
    client = FakeClient()
    storage = GCSStorage(BUCKET, client=client)
    payload = b"storage-contract-test"
    digest = hashlib.sha256(payload).hexdigest()
    expected_ref = f"vehicle_positions/{digest[:2]}/{digest}.pb"

    result = storage.save_raw_payload(payload, "vehicle_positions")

    assert result == (expected_ref, digest, "new")
    assert client.objects[expected_ref] == payload
    assert client.calls == [(expected_ref, "application/octet-stream")]


def test_existing_payload_is_not_replaced_or_verified():
    client = FakeClient()
    storage = GCSStorage(BUCKET, client=client)
    payload = b"storage-contract-test"
    digest = hashlib.sha256(payload).hexdigest()
    ref = f"vehicle_positions/{digest[:2]}/{digest}.pb"

    # Different old bytes deliberately demonstrate the accepted trust boundary:
    # "existing" does not mean old object content was verified.
    client.objects[ref] = b"old object content is not checked"

    result = storage.save_raw_payload(payload, "vehicle_positions")

    assert result == (ref, digest, "existing")
    assert client.objects[ref] == b"old object content is not checked"


def test_audit_collision_is_a_capture_failure(
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient()
    storage = GCSStorage(BUCKET, client=client)
    payload = make_feed()

    install_http(monkeypatch, lambda request: httpx.Response(200, content=payload))

    # Force a collision. Normal capture invocations generate independent UUIDs.
    fixed_id = "1" * 32
    monkeypatch.setattr(
        capture,
        "uuid4",
        lambda: SimpleNamespace(hex=fixed_id),
    )

    assert capture.capture_one("vehicle_positions", storage=storage) == 0
    original = client.objects[f"audit/capture_attempts/{fixed_id}.json"]

    assert capture.capture_one("vehicle_positions", storage=storage) == 3

    assert len(payload_objects(client)) == 1
    assert len(attempt_records(client)) == 1
    assert client.objects[f"audit/capture_attempts/{fixed_id}.json"] == original
    assert not (isolated_environment / "data").exists()

    output = capsys.readouterr()
    assert "audit_unavailable" in output.err
    assert_safe_output(client, output)


@pytest.mark.parametrize(
    ("scenario", "expected_code", "expected_outcome"),
    [
        ("success", 0, "success"),
        ("payload_failure", 1, "storage_error"),
        ("audit_failure", 3, None),
        ("http_401", 1, "http_error"),
        ("timeout", 1, "timeout"),
    ],
)
def test_capture_gcs_scenarios(
    scenario,
    expected_code,
    expected_outcome,
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient(
        fail_payload=scenario == "payload_failure",
        fail_audit=scenario == "audit_failure",
    )
    storage = GCSStorage(BUCKET, client=client)
    payload = make_feed()

    def handler(request):
        assert request.url.params["apikey"] == TEST_API_KEY

        if scenario == "timeout":
            raise httpx.ReadTimeout(
                f"untrusted timeout detail: {TEST_API_KEY}",
                request=request,
            )

        if scenario == "http_401":
            return httpx.Response(401, content=b"authentication refused")

        return httpx.Response(200, content=payload)

    install_http(monkeypatch, handler)

    code = capture.capture_one(
        "vehicle_positions",
        poll_cycle_id="test-cycle",
        storage=storage,
    )

    assert code == expected_code

    records = attempt_records(client)
    raw = payload_objects(client)
    output = capsys.readouterr()

    assert_safe_output(client, output)
    assert not (isolated_environment / "data").exists()

    if scenario == "audit_failure":
        assert records == []
        assert len(raw) == 1
        assert "audit_unavailable" in output.err
        assert "capture_started" in output.out
        assert "attempt_record=" not in output.out
        return

    assert len(records) == 1
    record = records[0]
    assert record["outcome"] == expected_outcome
    assert record["poll_cycle_id"] == "test-cycle"

    if scenario == "success":
        assert len(raw) == 1
        assert raw[record["payload_ref"]] == payload
        assert record["payload_sha256"] == hashlib.sha256(payload).hexdigest()
        assert record["payload_bytes"] == len(payload)
        assert record["entity_count"] == 1

        # Verify ordering: payload first, audit second.
        assert client.calls[0][0].endswith(".pb")
        assert client.calls[1][0].startswith("audit/capture_attempts/")
        assert "attempt_record=audit/capture_attempts/" in output.out
    else:
        assert raw == {}
        assert "payload_ref" not in record

    if scenario == "payload_failure":
        # Received content identity is retained even when persistence fails.
        assert record["payload_sha256"] == hashlib.sha256(payload).hexdigest()
        assert record["payload_bytes"] == len(payload)

    if scenario == "http_401":
        assert record["http_status"] == 401
        assert record["error_category"] == "authentication"

    if scenario == "timeout":
        assert "http_status" not in record


def test_repeated_payload_keeps_two_independent_attempts(
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient()
    storage = GCSStorage(BUCKET, client=client)
    payload = make_feed()

    install_http(monkeypatch, lambda request: httpx.Response(200, content=payload))

    first = capture.capture_one(
        "vehicle_positions",
        poll_cycle_id="cycle-1",
        storage=storage,
    )
    second = capture.capture_one(
        "vehicle_positions",
        poll_cycle_id="cycle-2",
        storage=storage,
    )

    assert first == second == 0
    records = attempt_records(client)

    assert len(payload_objects(client)) == 1
    assert len(records) == 2
    assert records[0]["attempt_id"] != records[1]["attempt_id"]
    assert records[0]["poll_cycle_id"] != records[1]["poll_cycle_id"]
    assert records[0]["payload_ref"] == records[1]["payload_ref"]
    assert records[0]["payload_sha256"] == records[1]["payload_sha256"]
    assert not (isolated_environment / "data").exists()

    output = capsys.readouterr()
    assert "storage=existing" in output.out
    assert_safe_output(client, output)


def test_poller_gcs_collects_all_three_feeds(
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient()
    configure_gcs(monkeypatch, client)
    requested = []

    def handler(request):
        assert request.url.params["apikey"] == TEST_API_KEY
        feed_type = request.url.path.lstrip("/")
        requested.append(feed_type)
        return httpx.Response(200, content=make_feed(feed_type))

    install_http(monkeypatch, handler)

    assert poller.main() == 0

    records = attempt_records(client)
    output = capsys.readouterr()

    assert requested == list(FEED_TYPES)
    assert len(payload_objects(client)) == 3
    assert len(records) == 3
    assert {record["feed_type"] for record in records} == set(FEED_TYPES)
    assert len({record["attempt_id"] for record in records}) == 3
    assert len({record["poll_cycle_id"] for record in records}) == 1

    # The real selector initializes storage only once per cycle.
    assert client.bucket_names == [BUCKET]
    assert "feeds_attempted=3" in output.out
    assert "feeds_failed=0" in output.out
    assert not (isolated_environment / "data").exists()
    assert_safe_output(client, output)


# Boundary 1: storage selection and initialization.


@pytest.mark.parametrize("backend", [None, "local"])
def test_local_selection_does_not_construct_gcs_client(backend, monkeypatch):
    if backend is None:
        monkeypatch.delenv("GTFS_RT_STORAGE_BACKEND")
    else:
        monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", backend)

    def forbidden_client():
        raise AssertionError("local mode must not initialize GCS")

    monkeypatch.setattr(gcs_sdk, "Client", forbidden_client)

    assert storage_module.configured_cloud_storage() is None


def test_gcs_selection_uses_configured_bucket(monkeypatch):
    client = FakeClient()
    configure_gcs(monkeypatch, client)

    selected = storage_module.configured_cloud_storage()

    assert isinstance(selected, GCSStorage)
    assert client.bucket_names == [BUCKET]
    assert client.objects == {}
    assert client.calls == []


@pytest.mark.parametrize(
    "scenario",
    ["unsupported_backend", "missing_bucket", "client_failure"],
)
def test_storage_initialization_failure_prevents_source_http(
    scenario,
    monkeypatch,
    isolated_environment,
    capsys,
):
    constructed = []

    def failing_client():
        constructed.append(True)
        raise RuntimeError(f"untrusted initialization detail: {TEST_API_KEY}")

    def forbidden_http(*args, **kwargs):
        raise AssertionError("source HTTP must not start")

    monkeypatch.setattr(gcs_sdk, "Client", failing_client)
    monkeypatch.setattr(capture.httpx, "Client", forbidden_http)

    if scenario == "unsupported_backend":
        monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "unsupported")
    elif scenario == "missing_bucket":
        monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "gcs")
        monkeypatch.delenv("GTFS_RT_GCS_BUCKET", raising=False)
    else:
        monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "gcs")
        monkeypatch.setenv("GTFS_RT_GCS_BUCKET", BUCKET)

    assert poller.main() == 3

    if scenario == "client_failure":
        assert constructed == [True]
    else:
        assert constructed == []

    output = capsys.readouterr()

    assert "poll_cycle_id=" in output.out
    assert "cycle_outcome=storage_initialization_error" in output.err
    assert "capture_started" not in output.out
    assert "attempt_record=" not in output.out
    assert "feeds_attempted=" not in output.out
    assert TEST_API_KEY not in output.out + output.err
    assert not (isolated_environment / "data").exists()


# Boundary 2: preserve evidence when Protobuf parsing fails.


def test_invalid_protobuf_keeps_raw_bytes_and_failure_audit(
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient()
    storage = GCSStorage(BUCKET, client=client)

    # An incomplete varint is deliberately invalid Protobuf.
    payload = b"\x80"
    install_http(monkeypatch, lambda request: httpx.Response(200, content=payload))

    assert (
        capture.capture_one(
            "vehicle_positions",
            poll_cycle_id="invalid-payload-cycle",
            storage=storage,
        )
        == 1
    )

    records = attempt_records(client)
    raw = payload_objects(client)
    assert len(records) == 1
    assert len(raw) == 1

    record = records[0]
    assert record["outcome"] == "invalid_protobuf"
    assert record["error_category"] == "invalid_protobuf"
    assert record["http_status"] == 200
    assert record["payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert record["payload_bytes"] == 1
    assert raw[record["payload_ref"]] == payload
    assert "entity_count" not in record

    assert client.calls[0][0].endswith(".pb")
    assert client.calls[1][0].startswith("audit/capture_attempts/")
    assert not (isolated_environment / "data").exists()
    assert_safe_output(client, capsys.readouterr())


# Boundary 3: one feed failure must not stop the remaining feeds.


@pytest.mark.parametrize(
    ("scenario", "expected_code", "expected_outcome"),
    [
        ("http_401", 1, "http_error"),
        ("payload_failure", 1, "storage_error"),
        ("audit_failure", 3, None),
    ],
)
def test_one_feed_failure_does_not_stop_other_gcs_feeds(
    scenario,
    expected_code,
    expected_outcome,
    monkeypatch,
    isolated_environment,
    capsys,
):
    client = FakeClient(
        fail_payload_prefix=(
            "trip_updates/" if scenario == "payload_failure" else None
        ),
        fail_audit_feed=("trip_updates" if scenario == "audit_failure" else None),
    )
    configure_gcs(monkeypatch, client)
    requested = []

    def handler(request):
        assert request.url.params["apikey"] == TEST_API_KEY
        feed_type = request.url.path.lstrip("/")
        requested.append(feed_type)

        if scenario == "http_401" and feed_type == "trip_updates":
            return httpx.Response(401)

        return httpx.Response(200, content=make_feed(feed_type))

    install_http(monkeypatch, handler)

    assert poller.main() == expected_code

    records = attempt_records(client)
    by_feed = {record["feed_type"]: record for record in records}
    output = capsys.readouterr()

    assert requested == list(FEED_TYPES)
    assert client.bucket_names == [BUCKET]
    assert output.out.count("capture_started ") == 3
    assert "feeds_attempted=3" in output.out
    assert "feeds_failed=1" in output.out
    assert len({record["poll_cycle_id"] for record in records}) == 1

    for feed_type in ("vehicle_positions", "service_alerts"):
        assert by_feed[feed_type]["outcome"] == "success"
        assert by_feed[feed_type]["payload_ref"] in client.objects

    if scenario == "audit_failure":
        assert len(records) == 2
        assert "trip_updates" not in by_feed
        assert len(payload_objects(client)) == 3
        assert "audit_unavailable" in output.err
    else:
        assert len(records) == 3
        assert by_feed["trip_updates"]["outcome"] == expected_outcome
        assert "payload_ref" not in by_feed["trip_updates"]
        assert len(payload_objects(client)) == 2

    assert len({record["attempt_id"] for record in records}) == len(records)
    assert not (isolated_environment / "data").exists()
    assert_safe_output(client, output)
