"""Bronze health: the conditions that should page someone, as a pure function.

Three symptoms, chosen because together they cover failures that are otherwise
silent:

- Freshness: the latest *successful* capture of each realtime feed in Bronze is
  older than the threshold. It catches a stopped collector, an expired API key
  (attempts keep arriving, successes stop) and a stalled ingestion alike.
  Static schedule captures are weeks apart and are not checked.
- Open quarantine: a file broke the source contract and nobody has replayed or
  waived it (bronze_autoloader.quarantine_state).
- Coverage: an audit in Landing, modified after the discovery start and settled,
  is in neither capture_attempts nor quarantine. Listing Landing itself (not what
  Auto Loader discovered) catches both "discovered but not written" and "never
  discovered" (uncovered_audits).

The health_check notebook gathers the inputs and fails its task on any problem,
so the job's failure notification delivers the alert.
"""

from datetime import timedelta

from bronze_records import REALTIME_FEEDS

# Refs named in the problem text; the run output lists them all.
NAMED_UNCOVERED = 3


def uncovered_audits(landing, done_refs, start, settled_before):
    """Landing audits Bronze should hold but does not, sorted.

    landing holds (audit_ref, modified_at) pairs, modified_at aware UTC. An audit
    is expected once it was modified after `start` (None: every audit) and
    before `settled_before`: a file the collector wrote during or just before the
    ingestion run is the next run's work, not a gap. done_refs are the audit_refs
    in capture_attempts and quarantine; waived files are in quarantine, so they
    count as covered.
    """
    done = set(done_refs)
    return sorted(
        {
            ref
            for ref, modified_at in landing
            if (start is None or modified_at > start)
            and modified_at < settled_before
            and ref not in done
        }
    )


def health_problems(
    last_success_by_feed, quarantine_counts, now, max_age, uncovered=()
):
    """Human-readable problems, empty when healthy.

    last_success_by_feed maps feed_type to its latest successful capture's
    completed_at_utc; quarantine_counts is bronze_autoloader.quarantine_summary;
    uncovered is uncovered_audits().
    """
    if not isinstance(max_age, timedelta) or max_age <= timedelta(0):
        raise ValueError("max_age must be a positive timedelta")
    problems = []
    for feed in REALTIME_FEEDS:
        last = last_success_by_feed.get(feed)
        if last is None:
            problems.append(f"{feed}: no successful capture in Bronze")
        elif now - last > max_age:
            minutes = int((now - last).total_seconds() // 60)
            problems.append(f"{feed}: last successful capture {minutes} min ago")
    if quarantine_counts.get("open", 0):
        problems.append(
            f"{quarantine_counts['open']} quarantined file(s) open; see QUARANTINE.md"
        )
    if uncovered:
        named = ", ".join(uncovered[:NAMED_UNCOVERED])
        more = len(uncovered) - NAMED_UNCOVERED
        problems.append(
            f"{len(uncovered)} Landing audit(s) not in Bronze: {named}"
            + (f" and {more} more" if more > 0 else "")
        )
    return problems
