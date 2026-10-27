"""Test the CI report gate with generated reports, not private source data."""

import runpy
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

CHECKS = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/check_ci.py")
)
validate_report = CHECKS["validate_report"]


def write_report(tmp_path: Path, *, skips=(), problem=None, empty=False) -> Path:
    suite = ET.Element("testsuite")
    if not empty:
        ET.SubElement(suite, "testcase", classname="tests.test_poller", name="ok")
    for module, name, reason in skips:
        case = ET.SubElement(suite, "testcase", classname=f"tests.{module}", name=name)
        ET.SubElement(case, "skipped", message=reason)
    if problem is not None:
        case = ET.SubElement(suite, "testcase", classname="tests.example", name="bad")
        ET.SubElement(case, problem)
    path = tmp_path / "report.xml"
    ET.ElementTree(suite).write(path, encoding="utf-8")
    return path


def test_passing_report_is_accepted(tmp_path):
    assert validate_report(write_report(tmp_path)) == (1, 0)


def test_five_known_snapshot_skips_are_accepted(tmp_path):
    skips = [
        (module, name, CHECKS["SNAPSHOT_SKIP_REASON"])
        for module, name in sorted(CHECKS["ALLOWED_SNAPSHOT_SKIPS"])
    ]
    assert validate_report(write_report(tmp_path, skips=skips)) == (1, 5)


@pytest.mark.parametrize("wrong_identity", [False, True])
def test_wrong_skip_reason_or_identity_is_rejected(tmp_path, wrong_identity):
    module, name = min(CHECKS["ALLOWED_SNAPSHOT_SKIPS"])
    reason = CHECKS["SNAPSHOT_SKIP_REASON"]
    if wrong_identity:
        name = "an_unexpected_skip"
    else:
        reason = "a different reason"
    with pytest.raises(ValueError, match="unexpected skipped test"):
        validate_report(write_report(tmp_path, skips=[(module, name, reason)]))


@pytest.mark.parametrize("problem", ["failure", "error"])
def test_failed_report_is_rejected(tmp_path, problem):
    with pytest.raises(ValueError, match="failures or errors"):
        validate_report(write_report(tmp_path, problem=problem))


def test_empty_report_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="no cases"):
        validate_report(write_report(tmp_path, empty=True))


def test_malformed_report_is_rejected(tmp_path):
    path = tmp_path / "report.xml"
    path.write_text("not xml", encoding="utf-8")
    with pytest.raises(ET.ParseError):
        validate_report(path)
