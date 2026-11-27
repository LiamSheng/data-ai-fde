"""Run the Clef scope router over scope_cases.yaml and report [2026-11-27].

    uv run --with pyyaml python agent-service/evals/run_scope_eval.py [--limit N]

Calls POST /v1/systemone with `clef:free` (0.0 cost; undocumented, so it may
disappear like the clef-flash promotion did on 2026-11-09). Paced under the free
pool's 10 RPM. Every answer is written to results/<UTC time>.jsonl (git-ignored)
so thresholds can be re-chosen offline without calling the API again.
"""

import argparse
import json
import os
import statistics
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import yaml

HERE = Path(__file__).parent
URL = "https://api.experientiallabs.ai/v1/systemone"
MODEL = "clef:free"
IN_SCOPE = ("transit", "weather_transit", "smalltalk")
SECONDS_PER_CALL = 60 / 9  # stay under 10 RPM, with margin for retries


def decide(request, message):
    body = {**request, "model": MODEL, "state": {"message": message}}
    for attempt in range(4):
        started = time.monotonic()
        call = urllib.request.Request(
            URL,
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {os.environ['EXPLABS_API_KEY']}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(call, timeout=60) as response:
                answer = json.loads(response.read())
            return answer, time.monotonic() - started
        except urllib.error.HTTPError as error:
            text = error.read().decode()
            # Retry only the rate limit; any other error (403 promo ended, 400 schema)
            # is a configuration problem and must stop the run, not be skipped.
            if error.code != 429 or "insufficient" in text:
                raise SystemExit(f"HTTP {error.code}: {text[:500]}") from None
            time.sleep(15 * (attempt + 1))
    raise SystemExit("Still rate limited after 4 attempts")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="first N cases only (smoke test)")
    args = parser.parse_args()

    request = json.loads((HERE / "clef_scope_request.json").read_text())
    request.pop("_comment")
    cases = yaml.safe_load((HERE / "scope_cases.yaml").read_text())["cases"][: args.limit]
    out_dir = HERE / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + ".jsonl")

    rows = []
    with out.open("w") as sink:
        for n, case in enumerate(cases, 1):
            tick = time.monotonic()
            answer, latency = decide(request, case["text"])
            intent = answer["answers"]["intent"]
            row = {
                **case,
                "choice": intent["choice"],
                "confidence": intent["confidence"],
                "p_in_scope": sum(intent["probabilities"][o] for o in IN_SCOPE),
                "probabilities": intent["probabilities"],
                "latency_s": round(latency, 3),
                "input_tokens": answer["usage"]["input_tokens"],
                "cost": answer["usage"].get("cost"),
            }
            rows.append(row)
            sink.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(f"{n:>3}/{len(cases)} {row['p_in_scope']:.2f} {row['choice']:<20} {case['text'][:50]}")
            time.sleep(max(0, SECONDS_PER_CALL - (time.monotonic() - tick)))

    report(rows, out)


def report(rows, out):
    print(f"\nResults: {out}")
    print(f"Option accuracy: {sum(r['choice'] == r['option'] for r in rows)}/{len(rows)}")
    print("\nScope accuracy by threshold (in_scope when p_in_scope >= t):")
    for t in (0.2, 0.3, 0.4, 0.5, 0.6, 0.7):
        wrong_reject = [r for r in rows if r["expected"] == "in_scope" and r["p_in_scope"] < t]
        wrong_accept = [r for r in rows if r["expected"] == "out_of_scope" and r["p_in_scope"] >= t]
        correct = len(rows) - len(wrong_reject) - len(wrong_accept)
        print(f"  t={t:.1f}  {correct}/{len(rows)}  wrongly rejected {len(wrong_reject)}  wrongly accepted {len(wrong_accept)}")
    print("\nOption errors and borderline cases (0.3 <= p_in_scope <= 0.7):")
    for r in rows:
        if r["choice"] != r["option"] or 0.3 <= r["p_in_scope"] <= 0.7:
            print(f"  p={r['p_in_scope']:.2f} conf={r['confidence']:.2f} expected={r['option']:<20} got={r['choice']:<20} {r['text']}")
    latencies = sorted(r["latency_s"] for r in rows)
    print(
        f"\nLatency p50 {statistics.median(latencies):.2f}s  p95 {latencies[int(0.95 * (len(latencies) - 1))]:.2f}s;"
        f" input tokens {sum(r['input_tokens'] for r in rows)}; cost {sum(r['cost'] or 0 for r in rows)}"
    )


if __name__ == "__main__":
    main()
