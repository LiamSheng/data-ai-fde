import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  buildView, parseLatest, relativeTime, captureProgress, isLoading, nextFetchDelay, nextClockTick,
  countStart, countFrame, easeOutExpo, QUICK_RETRY_MS, SLOW_RETRY_MS, CAPTURE_INTERVAL_MS,
} from '../src/lib/landing-status.ts';

// The contract's examples are the shared fixtures (data-platform/contracts/landing-status/examples).
const example = (name) => JSON.parse(readFileSync(
  new URL(`../../data-platform/contracts/landing-status/examples/${name}`, import.meta.url), 'utf8'));
const at = (iso, plusMs = 0) => new Date(Date.parse(iso) + plusMs);
const min = 60_000;
const texts = (view) => ({
  health: [view.health.text, view.health.tone, view.health.note],
  metrics: Object.fromEntries(Object.entries(view.metrics).map(([k, c]) => [k, [c.text, c.tone]])),
  rows: view.rows.map((r) => [r.name, r.transitData.text, r.transitData.tone, r.lastChecked.text, r.dataFrom.text, r.dataFrom.tone]),
});

test('no_previous: TU timed out, so Partial data with Not received, Error count and Error data time', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const view = texts(buildView(latest, at(latest.generated_at_utc, 3 * min)));
  assert.deepEqual(view.health, ['Partial data', 'error', null]);
  assert.deepEqual(view.metrics, {
    vehicles: ['1214', 'normal'], trips: ['Error', 'error'], alerts: ['87', 'normal'], maxLag: ['19 s', 'normal'],
  });
  assert.deepEqual(view.rows, [
    ['Vehicle positions', 'Received', 'normal', '3 min ago', '3 min ago', 'normal'],
    ['Trip updates', 'Not received', 'error', '3 min ago', 'Error', 'error'],
    ['Service alerts', 'Received', 'normal', '3 min ago', '3 min ago', 'normal'],
  ]);
});

test('after_sequence: SA audit failed, so Received but Partial data and SA data time behind in red', () => {
  const latest = parseLatest(example('latest.after_sequence.json'));
  const view = texts(buildView(latest, at(latest.generated_at_utc, 1 * min)));
  assert.deepEqual(view.health, ['Partial data', 'error', null]);
  const alerts = view.rows[2];
  assert.equal(alerts[1], 'Received');
  assert.equal(alerts[5], 'error');
});

test('a feed not requested this cycle shows its own, older last check', () => {
  const latest = parseLatest(example('latest.after_sequence.json'));
  const view = texts(buildView(latest, at(latest.generated_at_utc, 30_000)));
  // C4 requested only VP and TU; SA keeps its 18:20 attempt, ten minutes older than the file.
  assert.equal(view.rows[0][3], 'just now');
  assert.equal(view.rows[2][3], '10 min ago');
});

test('an expired file is Unavailable, keeps its values and turns last checks red', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const view = buildView(latest, at(latest.generated_at_utc, 200 * min));
  assert.deepEqual([view.health.text, view.health.note], ['Unavailable', 'Status file expired']);
  assert.equal(view.metrics.vehicles.text, '1214');
  assert.ok(view.rows.every((row) => row.lastChecked.text === '200 min ago' && row.lastChecked.tone === 'error'));
});

test('an unreachable file keeps the table and shows Error in red', () => {
  const view = buildView(null, new Date());
  assert.deepEqual([view.health.text, view.health.note], ['Unavailable', 'Status not reachable']);
  assert.deepEqual(view.rows.map((row) => row.name), ['Vehicle positions', 'Trip updates', 'Service alerts']);
  assert.ok([...Object.values(view.metrics), ...view.rows.flatMap((r) => [r.transitData, r.lastChecked, r.dataFrom])]
    .every((cell) => cell.text === 'Error' && cell.tone === 'error'));
});

test('a failed refresh keeps the old file in orange; errors stay red; expiry wins', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const kept = buildView(latest, at(latest.generated_at_utc, 11 * min), true);
  assert.deepEqual([kept.health.tone, kept.health.note], ['error', "Couldn't refresh, retrying"]);
  assert.equal(kept.metrics.vehicles.tone, 'warning');
  assert.equal(kept.metrics.trips.tone, 'error');
  const expired = buildView(latest, at(latest.generated_at_utc, 16 * min), true);
  assert.equal(expired.health.note, 'Status file expired');
  assert.equal(expired.metrics.vehicles.tone, 'normal');
});

test('a feed never attempted is N/A, and Max lag ignores it', () => {
  const latest = parseLatest(example('latest.after_sequence.json'));
  latest.feeds.trip_updates.last_attempt = null;
  const view = buildView(latest, at(latest.generated_at_utc, min));
  assert.deepEqual([view.metrics.trips.text, view.metrics.trips.tone], ['N/A', 'muted']);
  assert.equal(view.rows[1].transitData.text, 'N/A');
  for (const feed of Object.values(latest.feeds)) feed.last_attempt = null;
  assert.equal(buildView(latest, at(latest.generated_at_utc, min)).metrics.maxLag.text, 'N/A');
});

test('a healthy file is Normal', () => {
  const latest = parseLatest(example('latest.after_sequence.json'));
  latest.feeds.service_alerts = structuredClone(latest.feeds.vehicle_positions);
  assert.equal(buildView(latest, at(latest.generated_at_utc, min)).health.text, 'Normal');
});

test('parseLatest rejects an unknown version and a missing feed', () => {
  const latest = example('latest.no_previous.json');
  assert.throws(() => parseLatest({ ...latest, schema_version: 2 }), /schema_version/);
  const { trip_updates, ...feeds } = latest.feeds;
  assert.throws(() => parseLatest({ ...latest, feeds }), /trip_updates/);
  assert.throws(() => parseLatest(null), /schema_version/);
});

test('relative time is in whole minutes, "just now" under one', () => {
  const t = '2026-11-01T17:00:14Z';
  assert.equal(relativeTime(t, at(t, 59_999)), 'just now');
  assert.equal(relativeTime(t, at(t, 60_000)), '1 min ago');
  assert.equal(relativeTime(t, at(t, 200 * min + 30_000)), '200 min ago');
  assert.equal(relativeTime(t, at(t, -5_000)), 'just now');
});

test('the capture line fills over the interval from generated_at and loading starts ten seconds before due', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const g = latest.generated_at_utc;
  assert.equal(captureProgress(latest, at(g)), 0);
  assert.equal(captureProgress(latest, at(g, 5 * min)), 0.5);
  assert.equal(captureProgress(latest, at(g, 12 * min)), 1);
  assert.equal(captureProgress(latest, at(g, 15 * min)), 0);
  assert.equal(isLoading(latest, at(g, CAPTURE_INTERVAL_MS - 10_001)), false);
  assert.equal(isLoading(latest, at(g, CAPTURE_INTERVAL_MS - 10_000)), true);
  assert.equal(isLoading(latest, at(g, 14 * min)), true);
  assert.equal(isLoading(latest, at(g, 15 * min)), false);
  assert.equal(isLoading(null, new Date()), false);
});

test('requests: at the due time, then four 15 s retries, then one a minute', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const g = latest.generated_at_utc;
  assert.equal(nextFetchDelay(latest, at(g, 3 * min), 0), 7 * min);
  assert.deepEqual([0, 1, 2, 3, 4, 5].map((misses) => nextFetchDelay(latest, at(g, 11 * min), misses)),
    [QUICK_RETRY_MS, QUICK_RETRY_MS, QUICK_RETRY_MS, QUICK_RETRY_MS, SLOW_RETRY_MS, SLOW_RETRY_MS]);
  assert.equal(nextFetchDelay(latest, at(g, 20 * min), 9), SLOW_RETRY_MS);
  assert.equal(nextFetchDelay(null, new Date(), 0), QUICK_RETRY_MS);
});

test('count-up starts from the smallest number with as many digits and settles on the target', () => {
  assert.deepEqual([875, 14, 7, 1214].map(countStart), [100, 10, 0, 1000]);
  assert.equal(countFrame(100, 875, 0), 100);
  assert.equal(countFrame(100, 875, 1500), 875);
  assert.ok(countFrame(100, 875, 500) > 750, 'easeOutExpo covers most of the distance early');
  assert.equal(easeOutExpo(1), 1);
});

test('the clock ticks on whole minutes since generated_at and reads the true age after a long pause', () => {
  const latest = parseLatest(example('latest.no_previous.json'));
  const g = latest.generated_at_utc;
  assert.equal(nextClockTick(latest, at(g, 3 * min + 50_000)), 10_000);
  assert.equal(nextClockTick(latest, at(g, 3 * min)), min);
  assert.equal(nextClockTick(null, new Date()), min);
  // Timers held back for an hour: the next tick recomputes from the clock instead of adding one minute.
  const view = buildView(latest, at(g, 64 * min));
  assert.equal(view.rows[0].lastChecked.text, '64 min ago');
});
