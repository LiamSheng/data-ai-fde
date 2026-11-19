"""The capture audit contract, from Bronze's side (no Spark, no jsonschema).

Runs the shared examples in contracts/capture-audit/ through captured_audit():
every valid example is accepted, and every invalid one is quarantined with the
failed_check its manifest names, or accepted where the manifest says Bronze
reads tolerantly. The producer's side is tests/test_capture_audit_contract.py.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_records import BronzeContractError, captured_audit, parsed_audit

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "capture-audit"
EXAMPLES = CONTRACT / "examples"
INVALID = EXAMPLES / "invalid"
PREFIXES = {
    "capture_attempt": "capture_attempts",
    "schedule_capture_attempt": "schedule_capture_attempts",
}


def ref_for(path):
    audit = json.loads(path.read_text())
    return f"audit/{PREFIXES[path.name.split('.')[0]]}/{audit['attempt_id']}.json"


class ValidExamples(unittest.TestCase):
    def test_every_valid_example_is_accepted(self):
        paths = sorted(EXAMPLES.glob("*.json"))
        self.assertGreaterEqual(len(paths), 6)
        for path in paths:
            with self.subTest(path.name):
                audit, row = captured_audit(ref_for(path), path.read_bytes())
                self.assertEqual(row["attempt_id"], audit["attempt_id"])
                self.assertEqual(row["outcome"], audit["outcome"])
                # The version is kept with the whole document, not as a column.
                self.assertIn('"schema_version":1', row["raw_audit_json"])
                self.assertNotIn("schema_version", row)

    def test_a_failed_audit_keeps_the_payload_it_saved(self):
        path = EXAMPLES / "capture_attempt.invalid_protobuf.json"
        _, row = captured_audit(ref_for(path), path.read_bytes())
        self.assertEqual(row["outcome"], "invalid_protobuf")
        self.assertIsNotNone(row["payload_ref"])


class InvalidExamples(unittest.TestCase):
    def test_each_is_handled_as_the_manifest_says(self):
        cases = json.loads((INVALID / "manifest.json").read_text())
        self.assertTrue(cases)
        for case in cases:
            payload = (INVALID / case["file"]).read_bytes()
            with self.subTest(case["file"]):
                if case["bronze_check"] is None:
                    # Tolerant reader: accepted although the producer schema rejects it.
                    captured_audit(case["audit_ref"], payload)
                    continue
                with self.assertRaises(BronzeContractError) as raised:
                    captured_audit(case["audit_ref"], payload)
                self.assertEqual(str(raised.exception), case["bronze_check"])


class SchemaVersion(unittest.TestCase):
    def audit(self, **changes):
        path = EXAMPLES / "capture_attempt.http_error.json"
        audit = json.loads(path.read_text())
        audit.update(changes)
        return json.dumps(audit).encode()

    def test_missing_means_the_audits_written_before_the_field(self):
        audit = json.loads(self.audit())
        del audit["schema_version"]
        self.assertNotIn("schema_version", parsed_audit(json.dumps(audit).encode()))

    def test_one_is_accepted(self):
        self.assertEqual(parsed_audit(self.audit())["schema_version"], 1)

    def test_any_other_value_is_a_contract_failure(self):
        # bool and float compare equal to 1 in Python; the check is on the JSON type.
        for value in (2, 0, "1", True, 1.0):
            with self.subTest(value=value):
                with self.assertRaises(BronzeContractError) as raised:
                    parsed_audit(self.audit(schema_version=value))
                self.assertEqual(
                    str(raised.exception), "Unsupported audit schema version"
                )


if __name__ == "__main__":
    unittest.main()
