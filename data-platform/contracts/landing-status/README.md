# Landing status contract (v1)

Two JSON documents the collector writes at the end of every execution, so that
other components can tell whether collection is alive and whether each feed's
data is current without inferring it from individual audits. The collector owns
this contract; consumers (the web page, later Databricks) code against the
schemas and examples here.

| Document | Schema | Where | Write rule |
| --- | --- | --- | --- |
| Poll cycle record | `poll_cycle.v1.schema.json` | Landing bucket (private): `audit/poll_cycles/dt=<UTC date>/<YYYYMMDDTHHMMSSZ>_<poll_cycle_id>.json` | One per execution, create-only. A failed write makes the execution exit with code 3. |
| Latest status | `latest.v1.schema.json` | Status bucket (public read): `landing/v1/latest.json` | Overwritten each execution, best-effort. A failed write is logged and never changes the exit code. |

Local storage mode writes the same names under `data/ops/poll_cycles/` and
`data/status/landing/v1/latest.json`.

## Write order in one execution

1. Capture each requested feed. Each capture writes its own audit, as today.
2. Write the poll cycle record. The date directory and the time in the name
   come from `completed_at_utc`, so records list in time order within a day.
3. Read the previous `latest.json`, derive the new one, and write it. If the
   previous file is missing, malformed or has an unknown `schema_version`,
   start from empty. If reading it fails (a storage error), skip this cycle's
   write: starting from empty would erase the carried values. If the previous
   `generated_at_utc` is not older than this cycle's `completed_at_utc`, do
   not write.
4. If step 2 failed, still do step 3 with `cycle_ref: null`: the two documents
   live in different buckets, so the page can still see the collector running.
   If the record could not even be built, skip step 3 as well.

Local storage mode maps the `audit/` prefix to `data/ops/`, so `audit_ref` and
`cycle_ref` hold the same names in both modes.

## Rules

- **Every field is always present.** A missing value is `null`, never an
  omitted key. (Audit records omit empty fields; these documents do not.)
- **Timestamps** are RFC 3339 in UTC with a trailing `Z`. Fractional seconds
  are optional; readers must accept both forms.
- **A failed audit write does not hide the attempt.** The poll cycle record
  keeps the values the collector held in memory, marks them
  `audit_status: "failed"` with `audit_ref: null`, and keeps `payload_ref` and
  `payload_sha256` so the payload saved before the audit can still be found.
  These values are secondary evidence. No audit file is written afterwards.
  `failed` means the collector could not confirm the audit write, not that no
  audit exists: uploads are not retried, so one that timed out may still have
  landed. Readers must not treat a `failed` entry as proof of a missing audit.
- **Success** means `audit_status: "written"` and `outcome: "success"`.
- **`latest.json` is derived.** It can always be rebuilt from poll cycle
  records, so it never holds a fact the records do not.
  - `generated_at_utc` is the cycle's `completed_at_utc` (event time).
  - `stale_after_utc` is `generated_at_utc` + 1200 seconds (twice the longest
    poll interval). It will be revisited when intervals vary by time of day.
  - All three feeds are always present. `last_attempt` comes from this cycle
    when the feed was requested, otherwise from the previous file.
  - `last_success_at_utc` / `last_success_feed_timestamp` and
    `last_audit_failure_at_utc` / `last_audit_failure_poll_cycle_id` are
    carried over until a newer success or audit failure replaces them.
- **Rules a schema cannot express** (enforced when the collector builds each
  record, in the models' `__post_init__` in
  `src/transit_realtime_pipeline/status.py`, one test each):
  `feeds` in a poll cycle record matches `requested_feeds` one to one and in
  order; `audit_ref` names the same `attempt_id`; `payload_ref` starts with the
  feed type and the first two characters of `payload_sha256`;
  `stale_after_utc` is exactly 1200 seconds after `generated_at_utc`.

## Validation

- **Tests** (`tests/test_landing_status.py`): every example and every document
  the collector writes is validated against these schemas, and the latest
  examples are re-derived from the cycle examples. `jsonschema` is a
  development dependency only and is not in the runtime image.
- **Runtime**: both documents are built from dataclasses whose `__post_init__`
  rejects contradictory values, as `CaptureAttempt` already does. A poll cycle
  record that fails this check is not written (exit code 3); a `latest.json`
  that fails it is skipped and logged.
- **Readers** check what they read, at least `schema_version`, and treat an
  unknown version as unreadable.

## Versioning

Adding an optional field keeps `schema_version: 1`. Removing or renaming a
field, or changing what one means, moves to version 2. For `latest.json` a new
version gets a new path (`landing/v2/latest.json`) and both are written during
the transition. Poll cycle records carry their version in each record.

## Examples

All examples come from one scenario on 2026-10-30 (UTC), so they agree with
each other:

| File | Shows |
| --- | --- |
| `examples/poll_cycle.feed_failed.json` | 18:00 cycle: Trip Updates timed out, the other two succeeded |
| `examples/latest.no_previous.json` | `latest.json` built from that cycle with no previous file: Trip Updates has no success yet |
| `examples/poll_cycle.all_success.json` | 18:10 cycle: all three succeeded |
| `examples/poll_cycle.audit_failed.json` | 18:20 cycle: Service Alerts was captured but its audit could not be written |
| `examples/poll_cycle.feed_not_requested.json` | 18:30 cycle: only Vehicle Positions and Trip Updates were requested |
| `examples/latest.after_sequence.json` | `latest.json` after all four cycles: Service Alerts keeps its 18:20 attempt, its 18:10 success and its 18:20 audit failure |

How the web page uses `latest.json` is not decided yet; this contract only
fixes what the collector writes.
