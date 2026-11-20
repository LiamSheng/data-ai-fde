"""Bronze health: the conditions that should page someone, as a pure function.

Two symptoms, chosen because together they cover failures that are otherwise
silent:

- Freshness: the latest *successful* capture of each realtime feed in Bronze is
  older than the threshold. It catches a stopped collector, an expired API key
  (attempts keep arriving, successes stop) and a stalled ingestion alike.
  Static schedule captures are weeks apart and are not checked.
- Open quarantine: a file broke the source contract and nobody has replayed or
  waived it (bronze_autoloader.quarantine_state).

The health_check notebook gathers the inputs and fails its task on any problem,
so the job's failure notification delivers the alert.
"""

from datetime import timedelta

from bronze_records import REALTIME_FEEDS


def health_problems(last_success_by_feed, quarantine_counts, now, max_age):
    """Human-readable problems, empty when healthy.

    last_success_by_feed maps feed_type to its latest successful capture's
    completed_at_utc; quarantine_counts is bronze_autoloader.quarantine_summary.
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
    return problems
