# Quarantine runbook

A Landing file that breaks the capture audit contract
(`contracts/capture-audit/`) gets a `quarantined` event in
`<catalog>.<schema>_bronze.quarantine` and the run goes on. This page is what
to do next. Run every command from `data-platform/databricks`, deploying main
with `-t main` (the `dev` target belongs to the showcase branch).

## The table

Append-only events, one or more per audit file:

| `event` | Written by | `failed_check` holds |
| --- | --- | --- |
| `quarantined` | the stream or a replay that failed the file | the fixed message of the failed check |
| `resolved` | a replay that converted the file | null |
| `waived` | an operator giving the file up | the operator's reason |

A file is **open** from its first `quarantined` event until a `resolved` or
`waived` event closes it (`bronze_autoloader.quarantine_state`). Closed files
never reopen. Each run's output reports the counts as
`"quarantine": {"open": …, "resolved": …, "waived": …}`.

## 1. Triage

```sql
WITH events AS (SELECT * FROM workspace.transit_dev_bronze.quarantine),
closed AS (SELECT DISTINCT audit_ref FROM events WHERE event IN ('resolved', 'waived'))
SELECT failed_check, feed_type, parser_version, count(DISTINCT audit_ref) AS files,
       min(ingested_at_utc) AS first_seen, max(ingested_at_utc) AS last_seen
FROM events
WHERE event = 'quarantined' AND audit_ref NOT IN (SELECT audit_ref FROM closed)
GROUP BY ALL ORDER BY files DESC;
```

Read the `failed_check` against this table:

| Cause | Typical `failed_check` | Next step |
| --- | --- | --- |
| Bronze is wrong: a parser bug or a rule stricter than the contract | `Invalid audit field`, `Feed timestamp missing` on files that look fine | Fix the code, add the file as a test case, then **replay** |
| A payload was missing when the audit was read | `Payload missing for successful audit` | Restore it (the bucket keeps deleted objects 7 days), then **replay** |
| The collector wrote a wrong audit | `Payload SHA-256 mismatch`, `Audit time order is invalid`, `Unsupported audit schema version` | Fix the collector for future files. Landing objects are never rewritten, so the file itself can only be **waived** |
| The payload is corrupt | `Invalid GTFS-Realtime message`, `Unreadable Static ZIP`, `Oversized or empty payload` | **Waive** |

## 2. Replay

After a fix, re-run every open file through the same conversion:

```sh
databricks bundle deploy -t main
databricks bundle run -t main transit_bronze_autoloader --params mode=replay_quarantine
```

The output reports `unresolved_before`, `resolved`, `audit_missing` (an open
file whose audit is no longer in Landing) and `still_open`. A file that still
fails keeps its `quarantined` event; the same failure adds no new row.

If the replay fails with **Immutable key collision**, the fix changed what the
parser writes for a payload that Bronze already holds (payloads are
content-addressed, so another audit converted it under the old parser). Bronze
is insert-only and replay does not rewrite rows: reprocessing those rows is a
separate migration, for example a `parser_version` in the content-table keys or
a rebuild of the affected tables.

Silver views pick up resolved rows at once; rebuild Gold with `dbt build` for the
time range the files cover.

## 3. Waive

For a file that will never convert, record why and close it:

```sh
databricks bundle run -t main transit_bronze_autoloader --params \
  "mode=waive_quarantine,audit_refs=audit/capture_attempts/<id1>.json audit/capture_attempts/<id2>.json,waive_reason=Collector wrote a wrong payload hash before <commit>"
```

`--params` separates parameters with commas, so `audit_refs` separates refs with
spaces and the reason cannot contain a comma. The reason is one line of at most
200 characters. Only open files can be waived; a ref that is unknown or
already closed refuses the whole request and writes nothing. The `quarantined`
event and its failed check stay; replay skips the file from then on.
