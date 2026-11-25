"""Audit-driven Bronze ingestion: Auto Loader discovers audits into an inbox table,
then every pending audit is converted here in bounded chunks.

The collector writes a payload before its audit, so an audit Auto Loader finds
always refers to a payload already in Landing. Every audit becomes a
capture_attempts row, failed captures included; only successful ones have their
payload read and converted. A file that breaks the source contract is
quarantined and the chunk goes on; any other error fails the run, and its audits
stay pending for the next one, safely, because every write is an idempotent merge.
"""

import re
from datetime import UTC, datetime

from bronze_lib import KEYS, stamp
from bronze_records import (
    PARSER_VERSION,
    BronzeContractError,
    alert_records,
    calendar_records,
    captured_audit,
    digest,
    parsed_audit,
    route_records,
    trip_update_records,
    vehicle_records,
)

AUDIT_LIMIT = 128 * 1024
REALTIME_LIMIT = 16 * 1024 * 1024
STATIC_LIMIT = 32 * 1024 * 1024
AUDIT_PREFIXES = ("audit/capture_attempts/", "audit/schedule_capture_attempts/")


def _static_records(audit, payload):
    return {"routes": route_records(audit, payload), **calendar_records(audit, payload)}


# feed_type -> (payload extension, size limit, Bronze records by table)
FEEDS = {
    "vehicle_positions": (
        "pb",
        REALTIME_LIMIT,
        lambda a, p: {"vehicle_positions": vehicle_records(a, p)},
    ),
    "trip_updates": (
        "pb",
        REALTIME_LIMIT,
        lambda a, p: {"trip_updates": trip_update_records(a, p)},
    ),
    "service_alerts": (
        "pb",
        REALTIME_LIMIT,
        lambda a, p: {"service_alerts": alert_records(a, p)},
    ),
    "gtfs_schedule": ("zip", STATIC_LIMIT, _static_records),
}
# Content tables in write order; capture_attempts, the commit marker, comes last.
CONTENT_TABLES = (
    "vehicle_positions",
    "trip_updates",
    "service_alerts",
    "routes",
    "calendar",
    "calendar_dates",
)


def quarantine_event(ref, raw, feed_type, event, failed_check=None):
    return {
        "audit_ref": ref,
        "audit_sha256": digest(raw),
        "feed_type": feed_type,
        "event": event,
        "failed_check": failed_check,
        "parser_version": PARSER_VERSION,
    }


def _feed_type(raw):
    """Best-effort feed label for a quarantined file; None when unreadable."""
    try:
        feed = parsed_audit(raw).get("feed_type")
    except BronzeContractError:
        return None
    return feed if feed in FEEDS else None


def _payload_ref(audit):
    """The audit's payload path, checked before anything is read from it."""
    extension = FEEDS[audit["feed_type"]][0]
    ref = audit.get("payload_ref")
    pattern = rf"{audit['feed_type']}/[0-9a-f]{{2}}/[0-9a-f]{{64}}\.{extension}"
    if not isinstance(ref, str) or not re.fullmatch(pattern, ref):
        raise BronzeContractError("Content-addressed path mismatch")
    return ref


def _converted(audit, payload):
    _, limit, convert = FEEDS[audit["feed_type"]]
    if payload is None:
        raise BronzeContractError("Payload missing for successful audit")
    if not 0 < len(payload) <= limit:
        raise BronzeContractError("Oversized or empty payload")
    return convert(audit, payload)


def _ingest(landing, tables, files, now):
    """Convert then write one batch of (audit_ref, audit bytes); also the accepted refs."""
    accepted, quarantined, pending = {}, {}, []
    for ref, raw in sorted(files):
        try:
            audit, row = captured_audit(ref, raw)
            if audit["outcome"] == "success":
                _payload_ref(audit)
        except BronzeContractError as error:
            quarantined[ref] = quarantine_event(
                ref, raw, _feed_type(raw), "quarantined", str(error)
            )
            continue
        if audit["outcome"] == "success":
            pending.append((ref, raw, audit, row))
        else:
            accepted[ref] = row

    # Spark I/O stays outside the per-file try: a read failure is not a file's fault.
    payloads = landing.read_present(
        sorted({audit["payload_ref"] for _, _, audit, _ in pending})
    )
    content = {name: {} for name in CONTENT_TABLES}
    for ref, raw, audit, row in pending:
        try:
            converted = _converted(audit, payloads.get(audit["payload_ref"]))
        except BronzeContractError as error:
            quarantined[ref] = quarantine_event(
                ref, raw, audit["feed_type"], "quarantined", str(error)
            )
            continue
        for name, records in converted.items():
            keys = KEYS[name]
            for record in records:
                # Identical bytes captured twice yield identical rows; keep one.
                content[name].setdefault(tuple(record[k] for k in keys), record)
        accepted[ref] = row

    tables_written = {}
    for name in CONTENT_TABLES:
        if content[name]:
            tables_written[name] = tables.merge(
                name, stamp(list(content[name].values()), now)
            )
    if quarantined:
        tables_written["quarantine"] = tables.merge(
            "quarantine", stamp(list(quarantined.values()), now)
        )
    if accepted:
        tables_written["capture_attempts"] = tables.merge(
            "capture_attempts", stamp(list(accepted.values()), now)
        )
    result = {
        "audits": len(files),
        "failed_captures": sum(r["outcome"] != "success" for r in accepted.values()),
        "quarantined": len(quarantined),
        "tables": tables_written,
    }
    return result, sorted(accepted)


def ingest_audits(landing, tables, files, now=None):
    """One Auto Loader micro-batch: the discovered (audit_ref, bytes) pairs."""
    result, _ = _ingest(landing, tables, files, now or datetime.now(UTC))
    return result


WAIVE_REASON_LIMIT = 200


class WaiverError(ValueError):
    """An operator's waiver request that cannot be applied; its message is safe to show."""


def quarantine_state(events):
    """Each quarantined audit's state, derived from the append-only events.

    A file is open from its first 'quarantined' event until a 'resolved' event
    (replay converted it) or a 'waived' event (an operator gave it up) closes it.
    Nothing reopens a closed file: its audit_ref is in quarantine, so the stream
    never converts it again, and replay reads only open files. The latest
    'quarantined' event gives the audit's hash, feed and failed check.
    """
    by_ref = {}
    for event in events:
        by_ref.setdefault(event["audit_ref"], []).append(event)
    states = {}
    for ref, ref_events in by_ref.items():
        quarantined = [e for e in ref_events if e["event"] == "quarantined"]
        if not quarantined:
            continue
        kinds = {e["event"] for e in ref_events}
        latest = max(quarantined, key=lambda e: e["ingested_at_utc"])
        states[ref] = {
            "status": "resolved"
            if "resolved" in kinds
            else "waived"
            if "waived" in kinds
            else "open",
            "audit_sha256": latest["audit_sha256"],
            "feed_type": latest["feed_type"],
            "failed_check": latest["failed_check"],
        }
    return states


def quarantine_summary(tables):
    """Quarantined audits per state, for run output and the health check."""
    counts = {"open": 0, "resolved": 0, "waived": 0}
    for state in quarantine_state(tables.quarantine_events()).values():
        counts[state["status"]] += 1
    return counts


def open_quarantine(tables):
    """Audit refs still waiting in quarantine, sorted."""
    states = quarantine_state(tables.quarantine_events())
    return sorted(ref for ref, state in states.items() if state["status"] == "open")


def waive_quarantine(tables, refs, reason, now=None):
    """Close open quarantined audits that will never convert, with a reason.

    For a file that is truly corrupt or gone. It appends one 'waived' event per
    audit_ref, so replay stops retrying it and it no longer counts as open; the
    'quarantined' event and its failed check stay. The reason is kept in the
    event's failed_check column. Refs that are not open are refused, so a typo
    cannot record a waiver for a file that was never quarantined.
    """
    reason = reason.strip() if isinstance(reason, str) else ""
    if not reason or len(reason) > WAIVE_REASON_LIMIT or "\n" in reason:
        raise WaiverError(
            f"A waiver needs a one-line reason of at most {WAIVE_REASON_LIMIT} characters"
        )
    refs = sorted(set(refs))
    if not refs:
        raise WaiverError("Name at least one audit_ref to waive")
    states = quarantine_state(tables.quarantine_events())
    not_open = [ref for ref in refs if states.get(ref, {}).get("status") != "open"]
    if not_open:
        raise WaiverError(f"Not open in quarantine: {', '.join(not_open[:5])}")
    events = [
        {
            "audit_ref": ref,
            "audit_sha256": states[ref]["audit_sha256"],
            "feed_type": states[ref]["feed_type"],
            "event": "waived",
            "failed_check": reason,
            "parser_version": PARSER_VERSION,
        }
        for ref in refs
    ]
    merged = tables.merge("quarantine", stamp(events, now or datetime.now(UTC)))
    return {"waived": merged["inserted_rows"], "tables": {"quarantine": merged}}


def replay_quarantine(landing, tables, now=None):
    """Re-run open quarantined audits through the same conversion.

    Quarantine is append-only: a file that now converts gets a 'resolved' event,
    written after its capture_attempts row so a crash in between only means the
    next replay resolves it again. Waived files are not retried.
    """
    now = now or datetime.now(UTC)
    refs = open_quarantine(tables)
    raws = landing.read_present(refs)
    files = [(ref, raws[ref]) for ref in refs if ref in raws]
    result, accepted = _ingest(landing, tables, files, now)
    by_ref = dict(files)
    resolved = [
        quarantine_event(ref, by_ref[ref], _feed_type(by_ref[ref]), "resolved")
        for ref in accepted
    ]
    if resolved:
        result["tables"]["quarantine_resolved"] = tables.merge(
            "quarantine", stamp(resolved, now)
        )
    result.update(
        {
            "unresolved_before": len(refs),
            "audit_missing": len(refs) - len(files),
            "resolved": len(resolved),
            "still_open": len(refs) - len(resolved),
        }
    )
    return result


class SparkAuditLanding:
    """Landing objects under `root` (a gs:// prefix or a /Volumes/ test directory)."""

    def __init__(self, spark, root):
        if not root.endswith("/"):
            raise ValueError("Landing root must end with /")
        self.spark, self.root = spark, root

    def relative(self, path):
        # Spark reports Volume files as dbfs:/Volumes/...; the root is /Volumes/...
        if self.root.startswith("/") and path.startswith("dbfs:/"):
            path = path.removeprefix("dbfs:")
        if not path.startswith(self.root):
            raise RuntimeError("Discovered path outside the Landing root")
        return path.removeprefix(self.root)

    def _load(self, refs):
        frame = (
            self.spark.read.format("binaryFile")
            .load([self.root + ref for ref in refs])
            .select("path", "content")
        )
        return {
            self.relative(row["path"]): bytes(row["content"]) for row in frame.collect()
        }

    def read_present(self, refs):
        """Bytes of each ref that exists; a missing object is simply absent."""
        from pyspark.errors import AnalysisException

        found = {}
        for offset in range(0, len(refs), 500):
            chunk = refs[offset : offset + 500]
            try:
                found.update(self._load(chunk))
            except AnalysisException as error:
                if not _path_not_found(error):
                    raise
                # Rare: a referenced object is missing. Find which, one by one.
                for ref in chunk:
                    try:
                        found.update(self._load([ref]))
                    except AnalysisException as inner:
                        if not _path_not_found(inner):
                            raise
        return found


def _path_not_found(error):
    condition = getattr(error, "getCondition", None) or getattr(
        error, "getErrorClass", lambda: None
    )
    return condition() == "PATH_NOT_FOUND" or "Path does not exist" in str(error)


def pending_audits(inbox, done_refs, start=None):
    """Discovered audits not yet in capture_attempts or quarantine, oldest first.

    `inbox` holds (audit_ref, modified_ms) pairs, modified_ms being the file's
    modification time in epoch milliseconds. Every converted audit ends in exactly
    one of the two tables, so this is the work left after a crash as well as after
    new discoveries. With a start, audits modified at or before it are skipped even
    if they are in the inbox: an earlier checkpoint (or one that ignored
    modifiedAfter) may have discovered them, and discovery alone must not decide
    Bronze's coverage. The inbox can hold one ref more than once (every checkpoint
    version discovers it again); each ref is counted once. Returns (pending refs,
    number skipped by the start).
    """
    done = set(done_refs)
    start_ms = None if start is None else int(start.timestamp() * 1000)
    pending, skipped, seen = [], 0, set()
    for ref, modified_ms in inbox:
        if ref in done or ref in seen:
            continue
        seen.add(ref)
        if start_ms is not None and modified_ms <= start_ms:
            skipped += 1
            continue
        pending.append(ref)
    return pending, skipped


DISCOVER_ALL = "all"


def discover_options(discover_from_utc):
    """Auto Loader options that set where discovery starts, from an explicit choice.

    `all` takes every audit already in Landing (a full backfill); a UTC timestamp
    (YYYY-MM-DDTHH:MM:SSZ) takes only audits modified after it. There is no default:
    an empty value would silently mean one or the other, and the two differ in cost
    and in Bronze's coverage start. Auto Loader evaluates the start only when a
    checkpoint is first created, so changing it needs a new checkpoint version.
    """
    options = {"cloudFiles.includeExistingFiles": "true"}
    start = discover_start(discover_from_utc)
    if start is None:
        return options
    # Spark reads the value in the session time zone, which the notebook sets to UTC.
    options["modifiedAfter"] = start.strftime("%Y-%m-%dT%H:%M:%S")
    return options


def discover_start(discover_from_utc):
    """The aware UTC start, or None for `all`; anything else is refused."""
    if discover_from_utc == DISCOVER_ALL:
        return None
    try:
        return datetime.strptime(discover_from_utc, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
    except (TypeError, ValueError):
        raise ValueError(
            "discover_from_utc must be 'all' or a UTC time like 2026-11-25T00:00:00Z"
        ) from None


def discover(spark, inbox, landing_root, checkpoint_root, max_files, discover_from_utc):
    """Append newly arrived audit files (path and bytes) to the inbox Delta table.

    A plain Delta sink, not foreachBatch: on serverless (Spark Connect) a Python
    batch function must be pickled, and the protobuf descriptors the converters
    use cannot be. The sink gives exactly-once discovery; conversion runs after.
    """
    start_options = discover_options(discover_from_utc)
    for prefix in AUDIT_PREFIXES:
        query = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .options(**start_options)
            .option("cloudFiles.maxFilesPerTrigger", str(max_files))
            .option("pathGlobFilter", "*.json")
            .load(landing_root + prefix)
            .select("path", "modificationTime", "length", "content")
            .writeStream.option(
                "checkpointLocation", checkpoint_root + prefix.strip("/") + "/"
            )
            .trigger(availableNow=True)
            .toTable(inbox)
        )
        query.awaitTermination()


def run_autoloader(
    spark, namespace, landing_root, checkpoint_root, max_files, discover_from_utc
):
    """Discover with Auto Loader, then convert every pending audit in bounded chunks.

    Returns how many audits were converted and how many the start skipped. A
    failure leaves the rest pending; the next run continues, and rows already
    written merge as no-ops.
    """
    from bronze_lib import SparkTables
    from pyspark.sql import functions as F

    inbox = f"{namespace}.audit_inbox"
    discover(spark, inbox, landing_root, checkpoint_root, max_files, discover_from_utc)
    landing = SparkAuditLanding(spark, landing_root)
    tables = SparkTables(spark, namespace)
    done = []
    for table in ("capture_attempts", "quarantine"):
        if spark.catalog.tableExists(f"{namespace}.{table}"):
            done += [
                row[0]
                for row in spark.table(f"{namespace}.{table}")
                .select("audit_ref")
                .distinct()
                .collect()
            ]
    rows = (
        spark.table(inbox)
        # Epoch milliseconds: no session or driver time zone in the comparison.
        .select("path", F.expr("unix_millis(modificationTime)").alias("modified_ms"))
        .orderBy("modified_ms", "path")
        .collect()
    )
    paths = {landing.relative(row["path"]): row["path"] for row in rows}
    inbox_refs = [(landing.relative(r["path"]), r["modified_ms"]) for r in rows]
    pending, skipped = pending_audits(
        inbox_refs, done, discover_start(discover_from_utc)
    )
    for offset in range(0, len(pending), max_files):
        chunk = [paths[ref] for ref in pending[offset : offset + max_files]]
        rows = (
            spark.table(inbox)
            # A column of the same DataFrame: Spark Connect rejects one taken from a
            # second spark.table() call.
            .where(F.col("path").isin(chunk))
            .select("path", "content")
            .collect()
        )
        # The same file can be discovered twice (e.g. a reset checkpoint); keep one.
        files = {landing.relative(r["path"]): bytes(r["content"]) for r in rows}
        ingest_audits(landing, tables, sorted(files.items()))
    return {"converted_audits": len(pending), "skipped_before_start": skipped}
