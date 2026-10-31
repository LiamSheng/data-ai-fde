"""Poll cycle records and latest.json: contract, model checks and the poller.

HTTP is mocked; storage is local files or an in-memory fake. The contract and
its examples live in contracts/landing-status/.
"""

import copy
import json
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import jsonschema
import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline import capture, poller, status
from transit_realtime_pipeline import storage as storage_module
from transit_realtime_pipeline.config import FEED_TYPES, FEED_URL_ENV_VARS
from transit_realtime_pipeline.models import Outcome
from transit_realtime_pipeline.status import (
    CycleFeedAttempt,
    LatestStatus,
    PollCycleRecord,
)
from transit_realtime_pipeline.storage import (
    LATEST_STATUS_CACHE_CONTROL,
    LATEST_STATUS_OBJECT,
    GCSLatestStatusStore,
    GCSStorage,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "contracts" / "landing-status"
EXAMPLES = CONTRACT / "examples"
CYCLE_EXAMPLES = (
    "poll_cycle.feed_failed.json",
    "poll_cycle.all_success.json",
    "poll_cycle.audit_failed.json",
    "poll_cycle.feed_not_requested.json",
)
FAKE_KEY = "test-secret-value"
REVISION = "0123456789abcdef0123456789abcdef01234567"
# The unpatched client: serve() may be called more than once per test.
REAL_CLIENT = httpx.Client


def load(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def validator(kind: str) -> jsonschema.Draft202012Validator:
    schema = json.loads((CONTRACT / f"{kind}.v1.schema.json").read_text())
    return jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()
    )


def assert_valid(kind: str, document: dict) -> None:
    validator(kind).validate(document)


# The contract and its examples.


def test_schemas_are_valid_and_every_example_matches_its_schema() -> None:
    for kind in ("poll_cycle", "latest"):
        schema = json.loads((CONTRACT / f"{kind}.v1.schema.json").read_text())
        jsonschema.Draft202012Validator.check_schema(schema)
    for path in sorted(EXAMPLES.glob("*.json")):
        kind = path.name.split(".")[0]
        assert_valid(kind, json.loads(path.read_text()))


def test_cycle_examples_round_trip_through_the_model() -> None:
    for name in CYCLE_EXAMPLES:
        record = load(name)
        assert PollCycleRecord.from_record(record).to_record() == record


def test_latest_examples_are_derived_from_the_cycle_examples() -> None:
    cycles = [PollCycleRecord.from_record(load(name)) for name in CYCLE_EXAMPLES]
    latest = None
    derived = []
    for cycle in cycles:
        latest = LatestStatus.derive(latest, cycle, cycle.object_name)
        derived.append(latest)

    assert derived[0].to_record() == load("latest.no_previous.json")
    assert derived[-1].to_record() == load("latest.after_sequence.json")
    for name in ("latest.no_previous.json", "latest.after_sequence.json"):
        parsed = LatestStatus.from_json((EXAMPLES / name).read_bytes())
        assert parsed.to_record() == load(name)


def test_an_older_or_equal_cycle_does_not_overwrite_latest() -> None:
    cycles = [PollCycleRecord.from_record(load(name)) for name in CYCLE_EXAMPLES]
    latest = None
    for cycle in cycles:
        latest = LatestStatus.derive(latest, cycle, cycle.object_name)

    assert LatestStatus.derive(latest, cycles[1], None) is None
    assert LatestStatus.derive(latest, cycles[-1], None) is None


# Rules a schema cannot express, enforced by the models (one test each).


def test_audit_ref_must_name_the_same_attempt() -> None:
    record = load("poll_cycle.all_success.json")
    record["feeds"][0]["audit_ref"] = "audit/capture_attempts/" + "0" * 32 + ".json"
    assert_valid("poll_cycle", record)  # the schema alone accepts it
    with pytest.raises(ValueError, match="audit_ref"):
        PollCycleRecord.from_record(record)


@pytest.mark.parametrize("change", ["reorder", "drop"])
def test_feeds_must_match_requested_feeds_in_order(change: str) -> None:
    record = load("poll_cycle.all_success.json")
    if change == "reorder":
        record["feeds"].reverse()
    else:
        record["feeds"].pop()
    assert_valid("poll_cycle", record)
    with pytest.raises(ValueError, match="requested_feeds"):
        PollCycleRecord.from_record(record)


@pytest.mark.parametrize("change", ["feed_directory", "digest_prefix"])
def test_payload_ref_must_match_feed_type_and_digest(change: str) -> None:
    record = load("poll_cycle.all_success.json")
    entry = record["feeds"][0]
    digest = entry["payload_sha256"]
    if change == "feed_directory":
        entry["payload_ref"] = f"trip_updates/{digest[:2]}/{digest}.pb"
    else:
        other = "00" if digest[:2] != "00" else "11"
        entry["payload_ref"] = f"{entry['feed_type']}/{other}/{digest}.pb"
    assert_valid("poll_cycle", record)
    with pytest.raises(ValueError, match="payload_ref"):
        PollCycleRecord.from_record(record)


def test_stale_after_is_exactly_1200_seconds_after_generated_at() -> None:
    record = load("latest.after_sequence.json")
    record["stale_after_utc"] = record["stale_after_utc"].replace(":50:", ":49:")
    assert_valid("latest", record)
    with pytest.raises(ValueError, match="stale_after_utc"):
        LatestStatus.from_record(record)


def test_cycle_ref_must_name_this_cycles_record() -> None:
    record = load("latest.after_sequence.json")
    record["cycle_ref"] = record["cycle_ref"].replace("T183021Z", "T183020Z")
    assert_valid("latest", record)
    with pytest.raises(ValueError, match="cycle_ref"):
        LatestStatus.from_record(record)


def test_a_failed_audit_cannot_keep_an_audit_ref() -> None:
    record = load("poll_cycle.audit_failed.json")
    attempt = record["feeds"][2]
    attempt["audit_ref"] = f"audit/capture_attempts/{attempt['attempt_id']}.json"
    with pytest.raises(ValueError, match="audit_ref"):
        PollCycleRecord.from_record(record)


def test_success_needs_an_audit_to_count_as_last_success() -> None:
    cycle = PollCycleRecord.from_record(load("poll_cycle.audit_failed.json"))
    latest = LatestStatus.derive(None, cycle, cycle.object_name)
    alerts = latest.feeds["service_alerts"]

    assert alerts.last_attempt.outcome is Outcome.SUCCESS
    assert alerts.last_success_at_utc is None
    assert alerts.last_audit_failure_poll_cycle_id == cycle.poll_cycle_id


def test_jsonschema_is_a_development_dependency_only() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    runtime = " ".join(project["project"]["dependencies"])
    development = " ".join(project["dependency-groups"]["dev"])
    assert "jsonschema" not in runtime
    assert "jsonschema" in development


# The poller, local storage mode.

FEED_PATHS = {
    "vehicle_positions": "/positions",
    "trip_updates": "/trips",
    "service_alerts": "/alerts",
}


@pytest.fixture
def cycle_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", FAKE_KEY)
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "local")
    for name in (
        "GTFS_RT_GCS_BUCKET",
        "GTFS_RT_STATUS_BUCKET",
        poller.REQUESTED_FEEDS_ENV,
        poller.EXECUTION_NAME_ENV,
        poller.COLLECTOR_REVISION_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    for feed_type, path in FEED_PATHS.items():
        monkeypatch.setenv(FEED_URL_ENV_VARS[feed_type], f"https://example.test{path}")
    return tmp_path


def make_feed(feed_type: str) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(datetime(2026, 9, 29, tzinfo=UTC).timestamp())
    entity = feed.entity.add()
    entity.id = f"test-{feed_type}"
    if feed_type == "vehicle_positions":
        entity.vehicle.vehicle.id = "test-vehicle"
    elif feed_type == "trip_updates":
        entity.trip_update.trip.trip_id = "test-trip"
    else:
        entity.alert.header_text.translation.add().text = "Test alert"
    return feed.SerializeToString()


def serve(monkeypatch, *, timeout_feed: str | None = None) -> list[str]:
    """Answer every feed; optionally time one out. Returns the requested paths."""
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        feed_type = next(f for f, p in FEED_PATHS.items() if p == request.url.path)
        if feed_type == timeout_feed:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, content=make_feed(feed_type))

    def client(*args, **kwargs):
        return REAL_CLIENT(*args, transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(capture.httpx, "Client", client)
    return requested


def cycle_records(root: Path) -> list[dict]:
    paths = sorted((root / "data/ops/poll_cycles").rglob("*.json"))
    return [json.loads(path.read_text()) for path in paths]


def latest_path(root: Path) -> Path:
    return root / "data/status" / LATEST_STATUS_OBJECT


def read_latest(root: Path) -> dict:
    return json.loads(latest_path(root).read_text())


def by_feed(record: dict) -> dict[str, dict]:
    return {entry["feed_type"]: entry for entry in record["feeds"]}


def test_cycle_writes_a_record_and_latest_that_match_the_contract(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    assert poller.main() == 0

    [record] = cycle_records(cycle_dir)
    latest = read_latest(cycle_dir)
    assert_valid("poll_cycle", record)
    assert_valid("latest", latest)

    cycle = PollCycleRecord.from_record(record)
    path = cycle_dir / "data/ops" / Path(cycle.object_name).relative_to("audit")
    assert path.is_file()
    assert record["requested_feeds"] == list(FEED_TYPES)
    for entry in record["feeds"]:
        assert entry["audit_status"] == "written"
        assert (
            cycle_dir / "data/ops" / entry["audit_ref"].removeprefix("audit/")
        ).is_file()
        audit = json.loads(
            (
                cycle_dir / "data/ops" / entry["audit_ref"].removeprefix("audit/")
            ).read_text()
        )
        assert audit["poll_cycle_id"] == record["poll_cycle_id"]

    assert latest["generated_at_utc"] == record["completed_at_utc"]
    assert latest["cycle_ref"] == cycle.object_name
    assert record["execution_name"] is None
    assert record["collector_revision"] is None
    output = capsys.readouterr()
    assert "latest_status=written" in output.out
    assert FAKE_KEY not in output.out + output.err


def test_audit_failure_keeps_the_in_memory_attempt_without_an_audit_file(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    real_write = capture.write_attempt_record

    def write_except_trips(attempt, record_root):
        if attempt.feed_type == "trip_updates":
            raise OSError(f"audit failed with {FAKE_KEY}")
        return real_write(attempt, record_root)

    monkeypatch.setattr(capture, "write_attempt_record", write_except_trips)
    assert poller.main() == 3

    [record] = cycle_records(cycle_dir)
    assert_valid("poll_cycle", record)
    trips = by_feed(record)["trip_updates"]
    assert trips["audit_status"] == "failed"
    assert trips["audit_ref"] is None
    assert trips["outcome"] == "success"
    assert trips["entity_count"] == 1
    # The payload saved before the audit can still be found.
    assert (cycle_dir / "data/raw/gtfs_rt" / trips["payload_ref"]).is_file()
    audit_names = {p.stem for p in (cycle_dir / "data/ops/capture_attempts").glob("*")}
    assert trips["attempt_id"] not in audit_names

    latest = read_latest(cycle_dir)
    assert_valid("latest", latest)
    trip_status = latest["feeds"]["trip_updates"]
    assert trip_status["last_success_at_utc"] is None
    assert trip_status["last_audit_failure_poll_cycle_id"] == record["poll_cycle_id"]
    assert trip_status["last_audit_failure_at_utc"] == record["completed_at_utc"]
    output = capsys.readouterr()
    assert FAKE_KEY not in output.out + output.err + json.dumps(record)


def test_an_attempt_that_cannot_be_built_is_recorded_without_values(
    cycle_dir, monkeypatch
) -> None:
    serve(monkeypatch)
    real_attempt = capture.CaptureAttempt

    def refuse_trips(**fields):
        if fields["feed_type"] == "trip_updates":
            raise ValueError("cannot build")
        return real_attempt(**fields)

    monkeypatch.setattr(capture, "CaptureAttempt", refuse_trips)
    assert poller.main() == 3

    [record] = cycle_records(cycle_dir)
    assert_valid("poll_cycle", record)
    trips = by_feed(record)["trip_updates"]
    assert trips["audit_status"] == "failed"
    assert trips["outcome"] is None
    assert trips["attempt_completed_at_utc"] is None
    assert trips["payload_ref"] is None


def test_cycle_record_failure_exits_3_and_latest_is_still_written(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)

    def unavailable(root, name, contents):
        raise OSError(f"disk failed {FAKE_KEY}")

    monkeypatch.setattr(poller, "write_local_poll_cycle_record", unavailable)
    assert poller.main() == 3

    assert cycle_records(cycle_dir) == []
    latest = read_latest(cycle_dir)
    assert_valid("latest", latest)
    assert latest["cycle_ref"] is None
    assert len(list((cycle_dir / "data/ops/capture_attempts").glob("*.json"))) == 3
    output = capsys.readouterr()
    assert "poll_cycle_record_unavailable" in output.err
    assert FAKE_KEY not in output.out + output.err


def test_a_contradictory_cycle_record_is_not_written(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    monkeypatch.setattr(poller, "collector_revision", lambda: "not-a-commit")
    assert poller.main() == 3

    assert cycle_records(cycle_dir) == []
    assert not latest_path(cycle_dir).exists()
    output = capsys.readouterr()
    assert "poll_cycle_record_invalid" in output.err
    assert "latest_status=skipped_no_cycle_record" in output.err


def test_latest_write_failure_does_not_change_the_exit_code(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)

    def unavailable(self, contents):
        raise OSError(f"status failed {FAKE_KEY}")

    monkeypatch.setattr(storage_module.LocalLatestStatusStore, "write", unavailable)
    assert poller.main() == 0

    assert len(cycle_records(cycle_dir)) == 1
    assert not latest_path(cycle_dir).exists()
    output = capsys.readouterr()
    assert "latest_status=unavailable" in output.err
    assert FAKE_KEY not in output.out + output.err


def test_latest_that_fails_its_model_check_is_skipped_and_logged(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)

    def contradictory(previous, cycle, cycle_ref):
        raise ValueError("stale_after_utc must be generated_at_utc + 1200 s")

    monkeypatch.setattr(poller.LatestStatus, "derive", contradictory)
    assert poller.main() == 0

    assert len(cycle_records(cycle_dir)) == 1
    assert not latest_path(cycle_dir).exists()
    assert "latest_status=unavailable" in capsys.readouterr().err


def test_latest_read_failure_skips_the_write_instead_of_starting_over(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    assert poller.main() == 0
    before = latest_path(cycle_dir).read_bytes()

    def unreadable(self):
        raise OSError("read failed")

    monkeypatch.setattr(storage_module.LocalLatestStatusStore, "read", unreadable)
    assert poller.main() == 0

    assert len(cycle_records(cycle_dir)) == 2
    assert latest_path(cycle_dir).read_bytes() == before
    assert "latest_status=unavailable" in capsys.readouterr().err


def test_last_success_and_last_audit_failure_roll_over_later_cycles(
    cycle_dir, monkeypatch
) -> None:
    serve(monkeypatch)
    real_write = capture.write_attempt_record

    def fail_trips_once(attempt, record_root):
        if attempt.feed_type == "trip_updates" and not fail_trips_once.done:
            fail_trips_once.done = True
            raise OSError("audit failed")
        return real_write(attempt, record_root)

    fail_trips_once.done = False
    assert poller.main() == 0  # 1: all succeed
    first = read_latest(cycle_dir)["feeds"]["trip_updates"]

    monkeypatch.setattr(capture, "write_attempt_record", fail_trips_once)
    assert poller.main() == 3  # 2: trips captured, audit lost
    second = read_latest(cycle_dir)["feeds"]["trip_updates"]

    serve(monkeypatch, timeout_feed="trip_updates")
    assert poller.main() == 1  # 3: trips time out
    third = read_latest(cycle_dir)
    assert_valid("latest", third)
    trips = third["feeds"]["trip_updates"]

    assert trips["last_attempt"]["outcome"] == "timeout"
    # The success from cycle 1 survives an audit loss and a timeout.
    assert trips["last_success_at_utc"] == first["last_success_at_utc"]
    # The audit loss from cycle 2 survives the timeout in cycle 3.
    assert trips["last_audit_failure_at_utc"] == second["last_audit_failure_at_utc"]
    assert trips["last_audit_failure_poll_cycle_id"] is not None


def test_malformed_previous_latest_starts_from_empty(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    latest_path(cycle_dir).parent.mkdir(parents=True)
    latest_path(cycle_dir).write_text('{"schema_version": 1, "broken": true}')

    assert poller.main() == 0

    latest = read_latest(cycle_dir)
    assert_valid("latest", latest)
    assert latest["feeds"]["vehicle_positions"]["last_success_at_utc"] is not None
    output = capsys.readouterr()
    assert "latest_status_previous_malformed" in output.err
    assert "latest_status=written" in output.out


def test_a_newer_latest_is_not_overwritten_by_this_cycle(
    cycle_dir, monkeypatch, capsys
) -> None:
    serve(monkeypatch)
    future = copy.deepcopy(load("latest.after_sequence.json"))
    future.update(
        generated_at_utc="2099-01-01T00:00:00Z",
        stale_after_utc="2099-01-01T00:20:00Z",
        poll_cycle_id="f" * 32,
        cycle_ref=None,
    )
    contents = LatestStatus.from_record(future).to_json()
    latest_path(cycle_dir).parent.mkdir(parents=True)
    latest_path(cycle_dir).write_bytes(contents)

    assert poller.main() == 0

    assert latest_path(cycle_dir).read_bytes() == contents
    assert len(cycle_records(cycle_dir)) == 1
    assert "latest_status=skipped_not_newer" in capsys.readouterr().out


def test_requested_feeds_limit_the_cycle_and_the_rest_carry_over(
    cycle_dir, monkeypatch
) -> None:
    requested = serve(monkeypatch)
    assert poller.main() == 0
    [first] = cycle_records(cycle_dir)

    monkeypatch.setenv(poller.REQUESTED_FEEDS_ENV, "vehicle_positions, trip_updates")
    requested.clear()
    assert poller.main() == 0

    assert requested == ["/positions", "/trips"]
    second = next(r for r in cycle_records(cycle_dir) if r != first)
    assert second["requested_feeds"] == ["vehicle_positions", "trip_updates"]
    assert [entry["feed_type"] for entry in second["feeds"]] == second[
        "requested_feeds"
    ]

    latest = read_latest(cycle_dir)
    assert_valid("latest", latest)
    alerts = latest["feeds"]["service_alerts"]["last_attempt"]
    assert alerts["poll_cycle_id"] == first["poll_cycle_id"]
    trips = latest["feeds"]["trip_updates"]["last_attempt"]
    assert trips["poll_cycle_id"] == second["poll_cycle_id"]


@pytest.mark.parametrize(
    "selection", ["", "trip_updates,bogus", "trip_updates,trip_updates"]
)
def test_an_invalid_feed_selection_fails_before_any_request(
    selection, cycle_dir, monkeypatch, capsys
) -> None:
    requested = serve(monkeypatch)
    monkeypatch.setenv(poller.REQUESTED_FEEDS_ENV, selection)

    assert poller.main() == 2

    assert requested == []
    assert not (cycle_dir / "data").exists()
    assert "invalid_feed_selection" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("revision", "expected"),
    [(REVISION, REVISION), ("unrecorded", None), (REVISION.upper(), None)],
)
def test_execution_name_and_revision_are_recorded(
    revision, expected, cycle_dir, monkeypatch
) -> None:
    serve(monkeypatch)
    monkeypatch.setenv(poller.EXECUTION_NAME_ENV, "gtfs-rt-collector-abc12")
    monkeypatch.setenv(poller.COLLECTOR_REVISION_ENV, revision)

    assert poller.main() == 0

    [record] = cycle_records(cycle_dir)
    assert record["execution_name"] == "gtfs-rt-collector-abc12"
    assert record["collector_revision"] == expected


# GCS adapters, with in-memory fakes.


class FakeBlob:
    def __init__(self, bucket: "FakeBucket", name: str) -> None:
        self.bucket = bucket
        self.name = name
        self.cache_control = None

    def upload_from_string(self, data, *, content_type, timeout, retry, **kwargs):
        assert timeout > 0 and retry is None
        if self.bucket.fail_upload:
            raise RuntimeError(f"upload failed {FAKE_KEY}")
        precondition = kwargs.get("if_generation_match")
        if precondition == 0 and self.name in self.bucket.objects:
            raise PreconditionFailed("object already exists")
        self.bucket.objects[self.name] = bytes(data)
        self.bucket.uploads.append(
            (self.name, content_type, self.cache_control, precondition)
        )

    def download_as_bytes(self, *, timeout, retry):
        assert timeout > 0 and retry is None
        if self.bucket.fail_download:
            raise RuntimeError(f"download failed {FAKE_KEY}")
        if self.name not in self.bucket.objects:
            raise NotFound("missing")
        return self.bucket.objects[self.name]


class FakeBucket:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[tuple] = []
        self.fail_upload = False
        self.fail_download = False

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)


class FakeClient:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, name: str) -> FakeBucket:
        return self.buckets.setdefault(name, FakeBucket())


def test_gcs_poll_cycle_record_is_create_only() -> None:
    client = FakeClient()
    landing = GCSStorage("landing", client=client)
    cycle = PollCycleRecord.from_record(load("poll_cycle.all_success.json"))

    landing.write_poll_cycle_record(cycle.object_name, cycle.to_json())
    with pytest.raises(OSError, match="poll cycle record upload failed"):
        landing.write_poll_cycle_record(cycle.object_name, b"{}")

    bucket = client.buckets["landing"]
    assert bucket.objects[cycle.object_name] == cycle.to_json()
    assert bucket.uploads == [
        (cycle.object_name, "application/json", None, 0),
    ]


def test_gcs_latest_store_overwrites_without_caching() -> None:
    client = FakeClient()
    store = GCSLatestStatusStore("status", client=client)

    assert store.read() is None
    store.write(b"first")
    store.write(b"second")

    assert store.read() == b"second"
    uploads = client.buckets["status"].uploads
    assert [upload[0] for upload in uploads] == [LATEST_STATUS_OBJECT] * 2
    assert all(upload[2] == LATEST_STATUS_CACHE_CONTROL for upload in uploads)
    assert all(upload[3] is None for upload in uploads)


def test_gcs_latest_store_errors_are_redacted() -> None:
    client = FakeClient()
    store = GCSLatestStatusStore("status", client=client)
    client.buckets["status"].fail_download = True
    client.buckets["status"].fail_upload = True

    with pytest.raises(OSError) as read_error:
        store.read()
    with pytest.raises(OSError) as write_error:
        store.write(b"x")

    assert FAKE_KEY not in str(read_error.value) + str(write_error.value)
    assert read_error.value.__cause__ is None


def test_gcs_mode_without_a_status_bucket_skips_latest_only(
    cycle_dir, monkeypatch, capsys
) -> None:
    client = FakeClient()
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "gcs")
    monkeypatch.setenv("GTFS_RT_GCS_BUCKET", "landing")
    monkeypatch.setattr(
        storage_module, "GCSStorage", lambda name: GCSStorage(name, client=client)
    )
    serve(monkeypatch)

    assert poller.main() == 0

    landing = client.buckets["landing"].objects
    names = [name for name in landing if name.startswith(status.CYCLE_PREFIX)]
    assert len(names) == 1
    assert_valid("poll_cycle", json.loads(landing[names[0]]))
    assert set(client.buckets) == {"landing"}
    assert not (cycle_dir / "data").exists()
    assert "latest_status=unconfigured" in capsys.readouterr().err


def test_gcs_mode_writes_latest_to_the_status_bucket(
    cycle_dir, monkeypatch, capsys
) -> None:
    client = FakeClient()
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "gcs")
    monkeypatch.setenv("GTFS_RT_GCS_BUCKET", "landing")
    monkeypatch.setenv("GTFS_RT_STATUS_BUCKET", "status")
    monkeypatch.setattr(
        storage_module, "GCSStorage", lambda name: GCSStorage(name, client=client)
    )
    monkeypatch.setattr(
        storage_module,
        "GCSLatestStatusStore",
        lambda name: GCSLatestStatusStore(name, client=client),
    )
    serve(monkeypatch)

    assert poller.main() == 0

    latest = json.loads(client.buckets["status"].objects[LATEST_STATUS_OBJECT])
    assert_valid("latest", latest)
    assert latest["cycle_ref"] in client.buckets["landing"].objects
    assert LATEST_STATUS_OBJECT not in client.buckets["landing"].objects
    assert not (cycle_dir / "data").exists()
    assert "latest_status=written" in capsys.readouterr().out


def test_from_capture_rejects_a_mismatched_attempt() -> None:
    record = load("poll_cycle.all_success.json")
    entry = CycleFeedAttempt.from_record(record["feeds"][0])
    with pytest.raises(ValueError, match="without an attempt"):
        CycleFeedAttempt.from_capture(
            feed_type=entry.feed_type,
            attempt_id=entry.attempt_id,
            started_at_utc=entry.attempt_started_at_utc,
            attempt=None,
            audit_written=True,
        )
