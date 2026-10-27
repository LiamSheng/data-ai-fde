"""One-cycle tests with local Protobuf messages and mocked HTTP only."""

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline import capture, poller

FAKE_KEY = "test-secret-value"
FEED_PATHS = {
    "vehicle_positions": "/positions",
    "trip_updates": "/trips",
    "service_alerts": "/alerts",
}


@pytest.fixture
def isolated_cycle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GTFS_RT_API_KEY", FAKE_KEY)
    for feed_type, path in FEED_PATHS.items():
        env_name = {
            "vehicle_positions": "GTFS_RT_VEHICLE_POSITIONS_URL",
            "trip_updates": "GTFS_RT_TRIP_UPDATES_URL",
            "service_alerts": "GTFS_RT_SERVICE_ALERTS_URL",
        }[feed_type]
        monkeypatch.setenv(env_name, f"https://example.test{path}")
    return tmp_path


def make_feed(feed_type: str, *, empty: bool = False) -> bytes:
    """Build controlled GTFS-RT messages to test routing, not real-feed quality."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(datetime(2026, 9, 29, tzinfo=UTC).timestamp())

    if not empty:
        entity = feed.entity.add()
        entity.id = f"test-{feed_type}"
        if feed_type == "vehicle_positions":
            entity.vehicle.vehicle.id = "test-vehicle"
            entity.vehicle.position.latitude = 49.0
            entity.vehicle.position.longitude = -123.0
        elif feed_type == "trip_updates":
            entity.trip_update.trip.trip_id = "test-trip"
        elif feed_type == "service_alerts":
            entity.alert.header_text.translation.add().text = "Test alert"
            entity.alert.description_text.translation.add().text = "Test details"
        else:
            raise ValueError("unsupported test feed type")

    assert feed.IsInitialized()
    return feed.SerializeToString()


def install_transport(monkeypatch, handler) -> None:
    real_client = httpx.Client

    def client_with_mock_transport(*args, **kwargs):
        return real_client(
            *args,
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(capture.httpx, "Client", client_with_mock_transport)


def read_records(root: Path) -> dict[str, dict]:
    files = sorted((root / "data/ops/capture_attempts").glob("*.json"))
    records = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert all(
        path.stem == record["attempt_id"] for path, record in zip(files, records)
    )
    return {record["feed_type"]: record for record in records}


def assert_no_secret(root: Path, capsys) -> None:
    output = capsys.readouterr()
    records = list((root / "data/ops/capture_attempts").glob("*.json"))
    visible = output.out + output.err + "".join(p.read_text() for p in records)
    assert FAKE_KEY not in visible
    assert "https://example.test" not in visible


def test_one_cycle_captures_three_feeds_with_one_cycle_id(
    isolated_cycle, monkeypatch, capsys
) -> None:
    payloads = {feed: make_feed(feed) for feed in FEED_PATHS}
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.params["apikey"] == FAKE_KEY
        requested.append(request.url.path)
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=payloads[feed_type])

    install_transport(monkeypatch, respond)
    assert poller.main() == 0

    records = read_records(isolated_cycle)
    assert set(records) == set(FEED_PATHS)
    assert requested == list(FEED_PATHS.values())
    assert len({record["attempt_id"] for record in records.values()}) == 3
    assert len({record["poll_cycle_id"] for record in records.values()}) == 1

    count_fields = {
        "vehicle_positions": "vehicle_count",
        "trip_updates": "trip_update_count",
        "service_alerts": "alert_count",
    }
    for feed_type, record in records.items():
        assert record["outcome"] == "success"
        assert record["http_status"] == 200
        assert record["entity_count"] == 1
        assert record[count_fields[feed_type]] == 1
        assert record["payload_ref"].startswith(f"{feed_type}/")
        saved = isolated_cycle / "data/raw/gtfs_rt" / record["payload_ref"]
        assert saved.read_bytes() == payloads[feed_type]

    assert_no_secret(isolated_cycle, capsys)


def test_one_feed_timeout_does_not_stop_the_other_two(
    isolated_cycle, monkeypatch, capsys
) -> None:
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if request.url.path == FEED_PATHS["trip_updates"]:
            raise httpx.ReadTimeout(f"timed out at {request.url}", request=request)
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type, empty=True))

    install_transport(monkeypatch, respond)
    assert poller.main() == 1

    records = read_records(isolated_cycle)
    assert requested == list(FEED_PATHS.values())
    assert len({record["poll_cycle_id"] for record in records.values()}) == 1
    assert records["trip_updates"]["outcome"] == "timeout"
    assert "payload_ref" not in records["trip_updates"]
    for feed_type in ("vehicle_positions", "service_alerts"):
        assert records[feed_type]["outcome"] == "success"
        assert records[feed_type]["entity_count"] == 0
    assert_no_secret(isolated_cycle, capsys)


def test_one_feed_401_does_not_stop_the_other_two(
    isolated_cycle, monkeypatch, capsys
) -> None:
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if request.url.path == FEED_PATHS["trip_updates"]:
            return httpx.Response(401)
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type, empty=True))

    install_transport(monkeypatch, respond)
    assert poller.main() == 1

    records = read_records(isolated_cycle)
    assert requested == list(FEED_PATHS.values())
    assert records["trip_updates"]["outcome"] == "http_error"
    assert records["trip_updates"]["http_status"] == 401
    assert records["trip_updates"]["error_category"] == "authentication"
    assert records["vehicle_positions"]["outcome"] == "success"
    assert records["service_alerts"]["outcome"] == "success"
    assert_no_secret(isolated_cycle, capsys)


def test_missing_one_url_is_audited_without_blocking_other_feeds(
    isolated_cycle, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("GTFS_RT_TRIP_UPDATES_URL")
    requested: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type, empty=True))

    install_transport(monkeypatch, respond)
    assert poller.main() == 2

    records = read_records(isolated_cycle)
    assert requested == ["/positions", "/alerts"]
    assert records["trip_updates"]["outcome"] == "config_error"
    assert "http_status" not in records["trip_updates"]
    assert records["vehicle_positions"]["outcome"] == "success"
    assert records["service_alerts"]["outcome"] == "success"
    assert_no_secret(isolated_cycle, capsys)


def test_missing_key_creates_three_config_records_without_http(
    isolated_cycle, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("GTFS_RT_API_KEY")

    def must_not_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP must not run without an API key")

    install_transport(monkeypatch, must_not_request)
    assert poller.main() == 2

    records = read_records(isolated_cycle)
    assert set(records) == set(FEED_PATHS)
    assert len({record["poll_cycle_id"] for record in records.values()}) == 1
    assert all(record["outcome"] == "config_error" for record in records.values())
    assert all("http_status" not in record for record in records.values())
    assert_no_secret(isolated_cycle, capsys)


def test_wrong_entity_type_keeps_raw_payload_and_records_content_error(
    isolated_cycle, monkeypatch, capsys
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == FEED_PATHS["trip_updates"]:
            return httpx.Response(200, content=make_feed("vehicle_positions"))
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type, empty=True))

    install_transport(monkeypatch, respond)
    assert poller.main() == 1

    records = read_records(isolated_cycle)
    wrong = records["trip_updates"]
    assert wrong["outcome"] == "feed_content_error"
    assert wrong["error_category"] == "feed_content"
    assert wrong["http_status"] == 200
    assert wrong["vehicle_count"] == 1
    assert wrong["trip_update_count"] == 0
    assert wrong["payload_ref"].startswith("trip_updates/")
    saved = isolated_cycle / "data/raw/gtfs_rt" / wrong["payload_ref"]
    assert saved.read_bytes() == make_feed("vehicle_positions")
    assert_no_secret(isolated_cycle, capsys)


def test_repeated_cycles_make_six_attempts_but_only_three_payloads(
    isolated_cycle, monkeypatch, capsys
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type))

    install_transport(monkeypatch, respond)
    assert poller.main() == 0
    assert poller.main() == 0

    paths = list((isolated_cycle / "data/ops/capture_attempts").glob("*.json"))
    records = [json.loads(path.read_text()) for path in paths]
    assert len(records) == 6
    assert len({record["attempt_id"] for record in records}) == 6
    assert len({record["poll_cycle_id"] for record in records}) == 2
    assert len(list((isolated_cycle / "data/raw/gtfs_rt").rglob("*.pb"))) == 3
    assert_no_secret(isolated_cycle, capsys)


def test_unavailable_audit_for_one_feed_does_not_stop_cycle(
    isolated_cycle, monkeypatch, capsys
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        feed_type = next(
            feed for feed, path in FEED_PATHS.items() if path == request.url.path
        )
        return httpx.Response(200, content=make_feed(feed_type))

    install_transport(monkeypatch, respond)
    real_write = capture.write_attempt_record

    def write_except_one(attempt, record_root):
        if attempt.feed_type == "trip_updates":
            raise OSError(f"audit failed with {FAKE_KEY}")
        return real_write(attempt, record_root)

    monkeypatch.setattr(capture, "write_attempt_record", write_except_one)
    assert poller.main() == 3

    records = read_records(isolated_cycle)
    assert set(records) == {"vehicle_positions", "service_alerts"}
    output = capsys.readouterr()
    assert "audit_unavailable" in output.err
    assert "feeds_attempted=3" in output.out
    assert FAKE_KEY not in output.out + output.err
