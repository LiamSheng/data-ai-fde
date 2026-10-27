"""Credential-free checks shared by local verification and Cloud Build."""

import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

# These are optional private-snapshot checks, not generated-fixture checks.
# Both the test identity and skip reason must match; other skips fail the gate.
SNAPSHOT_SKIP_REASON = "local captured .pb is unavailable"
ALLOWED_SNAPSHOT_SKIPS = {
    ("test_capture", "test_real_snapshot_produces_payload_and_success_record"),
    ("test_capture", "test_repeated_snapshot_creates_two_attempts_but_one_payload"),
    ("test_capture", "test_storage_failure_is_audited_after_http_200"),
    (
        "test_capture_edge_cases",
        "test_valid_feed_without_timestamp_has_distinct_audit_outcome",
    ),
    (
        "test_capture_edge_cases",
        "test_valid_feed_with_zero_entities_is_still_a_successful_capture",
    ),
}


def validate_report(path: Path) -> tuple[int, int]:
    """Return pass/skip counts, rejecting empty, failed, or unexpected reports."""
    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    if not cases:
        raise ValueError("test report contains no cases")
    if list(root.iter("failure")) or list(root.iter("error")):
        raise ValueError("test report contains failures or errors")

    passed = 0
    skipped = 0
    for case in cases:
        skip = case.find("skipped")
        if skip is None:
            passed += 1
            continue
        identity = (
            case.get("classname", "").rsplit(".", 1)[-1],
            case.get("name", ""),
        )
        if (
            identity not in ALLOWED_SNAPSHOT_SKIPS
            or skip.get("message") != SNAPSHOT_SKIP_REASON
        ):
            raise ValueError("unexpected skipped test")
        skipped += 1

    if passed == 0:
        raise ValueError("test report contains no passing cases")
    return passed, skipped


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    # Do not let a developer's source configuration affect offline tests.
    for name in tuple(environment):
        if name.startswith("GTFS_RT_") or name == "GOOGLE_APPLICATION_CREDENTIALS":
            environment.pop(name)
    environment["GTFS_RT_STORAGE_BACKEND"] = "local"

    commands = [
        ["uv", "lock", "--check", "--offline"],
        [sys.executable, "-m", "ruff", "check", "."],
        [sys.executable, "-m", "ruff", "format", "--check", "."],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=root, env=environment, check=False)
        if result.returncode != 0:
            return 1

    with tempfile.TemporaryDirectory(prefix="transit-ci-") as report_directory:
        report_path = Path(report_directory) / "pytest.xml"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests",
                "-q",
                "-rs",
                f"--junitxml={report_path}",
            ],
            cwd=root,
            env=environment,
            check=False,
        )
        if result.returncode != 0:
            return 1
        try:
            passed, skipped = validate_report(report_path)
        except (OSError, ET.ParseError, ValueError):
            print("CI test-report validation failed", file=sys.stderr)
            return 1

    print(f"ci_tests_passed={passed}")
    print(f"ci_optional_snapshot_skips={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
