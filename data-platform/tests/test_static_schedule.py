"""Offline coverage for static GTFS download, archive storage and audit."""

import hashlib
import io
import json
import zipfile

import httpx
import pytest
from google.api_core.exceptions import PreconditionFailed

from transit_realtime_pipeline import static_schedule
from transit_realtime_pipeline.storage import GCSStorage

SCHEDULE_URL = "https://example.invalid/gtfs.zip"


def make_schedule_zip() -> bytes:
    """Build a small standards-shaped schedule fixture, not real feed data."""
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\na,Transit,https://example.invalid,America/Vancouver\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\ns1,Stop One,49.1,-122.6\n",
        "routes.txt": "route_id,agency_id,route_short_name,route_type\nr1,a,1,3\n",
        "trips.txt": "route_id,service_id,trip_id\nr1,wk,trip1\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\ntrip1,08:00:00,08:00:00,s1,1\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nwk,1,1,1,1,1,0,0,20261001,20261231\n",
        "calendar_dates.txt": "service_id,date,exception_type\nwk,20261012,1\n",
        "feed_info.txt": "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,feed_start_date,feed_end_date\nTransit,https://example.invalid,en,fixture-v1,20261001,20261231\n",
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)
    return buffer.getvalue()


@pytest.fixture(autouse=True)
def isolated_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_SCHEDULE_URL", SCHEDULE_URL)
    monkeypatch.setenv("GTFS_RT_STORAGE_BACKEND", "local")
    monkeypatch.delenv("GTFS_RT_GCS_BUCKET", raising=False)
    return tmp_path


def install_http(monkeypatch, handler):
    real_client = httpx.Client
    transport = httpx.MockTransport(handler)

    def client_factory(*args, **kwargs):
        return real_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(static_schedule.httpx, "Client", client_factory)


def audit_records(root):
    paths = sorted((root / "data/audit/schedule_capture_attempts").glob("*.json"))
    return [json.loads(path.read_text(encoding="utf-8")) for path in paths]


def test_archive_inspection_reports_files_version_and_calendar_dates():
    info = static_schedule.inspect_schedule_archive(make_schedule_zip())

    assert "trips.txt" in info.files
    assert info.feed_version == "fixture-v1"
    assert (info.feed_start_date, info.feed_end_date) == ("20261001", "20261231")
    assert (info.calendar_start_date, info.calendar_end_date) == (
        "20261001",
        "20261231",
    )
    assert (
        info.calendar_exception_add_start_date,
        info.calendar_exception_add_end_date,
    ) == ("20261012", "20261012")


def test_valid_download_saves_zip_and_independent_audit(
    isolated_run, monkeypatch, capsys
):
    payload = make_schedule_zip()

    def respond(request):
        assert request.method == "GET"
        assert str(request.url) == SCHEDULE_URL
        return httpx.Response(200, content=payload)

    install_http(monkeypatch, respond)

    assert static_schedule.capture_schedule_once() == 0

    record = audit_records(isolated_run)[0]
    digest = hashlib.sha256(payload).hexdigest()
    assert record["outcome"] == "success"
    assert record["feed_type"] == "gtfs_schedule"
    assert record["http_status"] == 200
    assert record["payload_ref"] == f"gtfs_schedule/{digest[:2]}/{digest}.zip"
    assert record["payload_sha256"] == digest
    assert record["payload_bytes"] == len(payload)
    assert record["feed_version"] == "fixture-v1"
    assert record["calendar_start_date"] == "20261001"
    assert record["calendar_end_date"] == "20261231"
    assert len(record["archive_files"]) == 8
    assert (isolated_run / "data/raw" / record["payload_ref"]).read_bytes() == payload
    assert SCHEDULE_URL not in capsys.readouterr().out


def test_identical_downloads_create_two_audits_but_one_archive(
    isolated_run, monkeypatch, capsys
):
    payload = make_schedule_zip()
    requests = 0

    def respond(request):
        nonlocal requests
        requests += 1
        return httpx.Response(200, content=payload)

    install_http(monkeypatch, respond)

    assert static_schedule.capture_schedule_once() == 0
    assert static_schedule.capture_schedule_once() == 0

    records = audit_records(isolated_run)
    archives = list((isolated_run / "data/raw/gtfs_schedule").rglob("*.zip"))
    assert requests == 2
    assert len(records) == 2
    assert records[0]["attempt_id"] != records[1]["attempt_id"]
    assert {record["payload_sha256"] for record in records} == {
        hashlib.sha256(payload).hexdigest()
    }
    assert {record["storage_disposition"] for record in records} == {"new", "existing"}
    assert len(archives) == 1
    assert "storage=existing" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("status", "category"),
    [(404, "http_client"), (500, "http_server"), (403, "authentication")],
)
def test_http_failures_write_audit_without_archive(
    isolated_run, monkeypatch, status, category
):
    install_http(
        monkeypatch,
        lambda request: httpx.Response(status, content=b"not a schedule archive"),
    )

    assert static_schedule.capture_schedule_once() == 1

    record = audit_records(isolated_run)[0]
    assert record["outcome"] == "http_error"
    assert record["http_status"] == status
    assert record["error_category"] == category
    assert "payload_ref" not in record
    assert list((isolated_run / "data/raw/gtfs_schedule").rglob("*.zip")) == []


def test_timeout_audit_does_not_include_request_url(isolated_run, monkeypatch, capsys):
    def time_out(request):
        raise httpx.ReadTimeout(f"request timeout for {request.url}", request=request)

    install_http(monkeypatch, time_out)

    assert static_schedule.capture_schedule_once() == 1

    record = audit_records(isolated_run)[0]
    output = capsys.readouterr()
    assert record["outcome"] == "timeout"
    assert record["error_category"] == "timeout"
    assert SCHEDULE_URL not in json.dumps(record) + output.out + output.err


def test_invalid_download_url_is_audited_without_echoing_it(
    isolated_run, monkeypatch, capsys
):
    unsafe_url = "https://example.invalid/gtfs.zip?token=do-not-log"
    monkeypatch.setenv("GTFS_SCHEDULE_URL", unsafe_url)

    assert static_schedule.capture_schedule_once() == 1

    record = audit_records(isolated_run)[0]
    output = capsys.readouterr()
    assert record["outcome"] == "config_error"
    assert record["error_category"] == "configuration"
    assert unsafe_url not in json.dumps(record) + output.out + output.err


def test_storage_failure_is_audited_without_exception_text(
    isolated_run, monkeypatch, capsys
):
    payload = make_schedule_zip()
    install_http(
        monkeypatch,
        lambda request: httpx.Response(200, content=payload),
    )

    def fail_save(payload_bytes, storage_root):
        raise OSError(f"storage failure for {SCHEDULE_URL}")

    monkeypatch.setattr(static_schedule, "save_local_schedule_archive", fail_save)

    assert static_schedule.capture_schedule_once() == 1

    record = audit_records(isolated_run)[0]
    output = capsys.readouterr()
    assert record["outcome"] == "storage_error"
    assert record["error_category"] == "storage"
    assert record["http_status"] == 200
    assert record["payload_bytes"] == len(payload)
    assert "payload_ref" not in record
    assert SCHEDULE_URL not in json.dumps(record) + output.out + output.err


def test_audit_write_failure_returns_distinct_nonzero_result(
    isolated_run, monkeypatch, capsys
):
    payload = make_schedule_zip()
    install_http(
        monkeypatch,
        lambda request: httpx.Response(200, content=payload),
    )

    def fail_audit(record, record_root):
        raise OSError(f"audit failure for {SCHEDULE_URL}")

    monkeypatch.setattr(static_schedule, "write_local_schedule_attempt", fail_audit)

    assert static_schedule.capture_schedule_once() == 3

    output = capsys.readouterr()
    assert "audit_unavailable" in output.err
    assert SCHEDULE_URL not in output.out + output.err
    assert audit_records(isolated_run) == []
    assert list((isolated_run / "data/raw/gtfs_schedule").rglob("*.zip"))


def test_invalid_zip_is_preserved_and_audited_as_invalid(isolated_run, monkeypatch):
    payload = b"HTTP 200 but not a ZIP"
    install_http(monkeypatch, lambda request: httpx.Response(200, content=payload))

    assert static_schedule.capture_schedule_once() == 1

    record = audit_records(isolated_run)[0]
    assert record["outcome"] == "invalid_archive"
    assert record["error_category"] == "invalid_archive"
    assert record["payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert (isolated_run / "data/raw" / record["payload_ref"]).read_bytes() == payload


def test_zip_without_required_gtfs_tables_is_invalid():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("readme.txt", "not a complete schedule feed")

    with pytest.raises(static_schedule.InvalidScheduleArchive):
        static_schedule.inspect_schedule_archive(buffer.getvalue())


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
        assert if_generation_match == 0
        assert timeout > 0
        assert retry is None
        self.client.calls.append((self.name, content_type))
        if self.name in self.client.objects:
            raise PreconditionFailed("exists")
        self.client.objects[self.name] = bytes(data)


class FakeBucket:
    def __init__(self, client):
        self.client = client

    def blob(self, name):
        return FakeBlob(self.client, name)


class FakeClient:
    def __init__(self):
        self.objects = {}
        self.calls = []

    def bucket(self, name):
        assert name == "landing-bucket"
        return FakeBucket(self)


def test_gcs_storage_uses_static_prefix_and_audit_path():
    client = FakeClient()
    storage = GCSStorage("landing-bucket", client=client)
    payload = make_schedule_zip()
    digest = hashlib.sha256(payload).hexdigest()

    first = storage.save_schedule_archive(payload)
    second = storage.save_schedule_archive(payload)
    record_ref = storage.write_schedule_attempt_record(
        {"attempt_id": "attempt-1", "outcome": "success"}
    )

    expected_ref = f"gtfs_schedule/{digest[:2]}/{digest}.zip"
    assert first == (expected_ref, digest, "new")
    assert second == (expected_ref, digest, "existing")
    assert record_ref == "audit/schedule_capture_attempts/attempt-1.json"
    assert client.objects[expected_ref] == payload
    assert json.loads(client.objects[record_ref]) == {
        "attempt_id": "attempt-1",
        "outcome": "success",
    }
