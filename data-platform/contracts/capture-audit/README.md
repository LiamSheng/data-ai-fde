# Capture audit contract (v1)

The JSON document each collector writes after every capture attempt, and the
only thing Bronze discovers: Auto Loader watches the two audit prefixes, and a
payload is read only through an audit's `payload_ref`. The collectors own this
contract (producer); Bronze is its consumer and verifies it instead of trusting
it, sending a file that breaks it to `quarantine`.

| Document | Schema | Where | Producer |
| --- | --- | --- | --- |
| Realtime capture attempt | `capture_attempt.v1.schema.json` | `audit/capture_attempts/<attempt_id>.json` | `CaptureAttempt.to_record()` (`models.py`) |
| Static schedule capture attempt | `schedule_capture_attempt.v1.schema.json` | `audit/schedule_capture_attempts/<attempt_id>.json` | `ScheduleAttempt.to_record()` (`schedule_models.py`) |

Both are written once (`if_generation_match=0`) and never changed.

## Rules

- **Empty fields are omitted, never `null`.** (The landing-status documents do
  the opposite; their readers need every key.)
- **Timestamps** are RFC 3339 in UTC with a trailing `Z`.
- **A success saved what it fetched.** Success means a 2xx response, a saved
  payload and, for realtime feeds, the parsed feed header: the schema lists the
  fields a success must carry and forbids `error_category` on it.
- **A failure says why.** Every other outcome carries `error_category`. A failed
  audit makes no payload promise, though it may still name a payload saved
  before the failure (for example `invalid_protobuf`), so it can be traced.
- **`schema_version`** is `1`. Audits written before the field existed have no
  `schema_version` and the same v1 shape; readers treat a missing value as v1.

## What the schema checks, and what only Bronze can

The schema checks one document: field names, types, enums, formats and the
success/failure rules. Some rules span the document's name, a second file or
time, and JSON Schema cannot express them. Bronze checks those in code
(`bronze_records.py`):

| Rule | Where it is checked |
| --- | --- |
| `attempt_id` equals the object name | `captured_audit()`: *Audit identity mismatch* |
| The directory allows the `feed_type` | `captured_audit()`: *Unexpected feed type for audit path* |
| `completed_at_utc` is not before `started_at_utc` | `_audit_times()`: *Audit time order is invalid* |
| `payload_sha256` and `payload_bytes` match the payload, and `payload_ref` is its content address | `verify_payload()` |
| `feed_timestamp`, `incrementality`, `feed_age_seconds` and the counts match the protobuf | `verified_feed()` |
| The payload was written before the audit, and neither changes afterwards | the collectors' write order and create-only uploads |

`examples/invalid/manifest.json` lists, for each invalid example, whether the
schema rejects it and which `failed_check` Bronze quarantines it with.

## Producer strict, reader tolerant

The schemas are closed (`additionalProperties: false`): the producer may emit
only what is listed. Bronze reads tolerantly: it checks only what it relies on
and keeps the whole document in `capture_attempts.raw_audit_json`. So an
outcome name Bronze has never seen is accepted as a failure
(`examples/invalid/unknown_outcome.json`), while the producer's own tests
reject it.

## Changing the contract

- **Compatible** (stays v1): a new optional field. Add it to the schema and an
  example; readers that ignore it keep working.
- **Breaking** (new version, new schema file): renaming or removing a field,
  changing a type, making a field required, or changing what a value means.
  Deploy Bronze first, accepting both versions; then the collectors. Bronze
  quarantines a `schema_version` it does not know
  (*Unsupported audit schema version*) rather than guessing.

## Examples

| File | Shows |
| --- | --- |
| `capture_attempt.success.json` | a real Trip Updates success (Nov 5, 2026) |
| `capture_attempt.http_error.json` | HTTP 500: a failure with no payload, accepted into `capture_attempts` |
| `capture_attempt.timeout.json` | a failure before any HTTP status |
| `capture_attempt.invalid_protobuf.json` | a failure that still names its saved payload |
| `schedule_capture_attempt.success.json` | a schedule archive download (values illustrative) |
| `schedule_capture_attempt.http_error.json` | a schedule download that failed |

Tests: `tests/test_capture_audit_contract.py` (producer) and
`databricks/ingestion/tests/test_audit_contract.py` (Bronze).
