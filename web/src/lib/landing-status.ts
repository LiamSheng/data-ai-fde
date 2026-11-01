/**
 * The Landing status bar's rules, kept free of the DOM so they can be tested with the contract's examples.
 * Input: landing/v1/latest.json (data-platform/contracts/landing-status/). Rules: PLANNING section 2 [2026-11-01], KAN-27.
 */

export const STATUS_URL = 'https://storage.googleapis.com/transit-realtime-status-26/landing/v1/latest.json';

/** Shown as "10 min". Hardcoded until poll intervals vary by feed (KAN-26). */
export const CAPTURE_INTERVAL_MS = 10 * 60_000;
/** "Now loading…" starts this long before the next file is due. */
export const LOADING_LEAD_MS = 10_000;
/** After the due time: four quick retries, then one a minute until a new file arrives or the old one expires. */
export const QUICK_RETRY_MS = 15_000;
export const QUICK_RETRIES = 4;
export const SLOW_RETRY_MS = 60_000;
/** The table's times are recomputed every second from the system clock, without a request (see nextClockTick). */
export const CLOCK_TICK_MS = 1_000;

export const FEEDS = [
  { type: 'vehicle_positions', name: 'Vehicle positions' },
  { type: 'trip_updates', name: 'Trip updates' },
  { type: 'service_alerts', name: 'Service alerts' },
] as const;

export type FeedType = (typeof FEEDS)[number]['type'];

export interface LastAttempt {
  cycle_completed_at_utc: string;
  audit_status: 'written' | 'failed';
  outcome: string | null;
  feed_age_seconds: number | null;
  entity_count: number | null;
}

export interface FeedStatus {
  last_attempt: LastAttempt | null;
  last_success_at_utc: string | null;
  last_success_feed_timestamp: string | null;
}

export interface LatestStatus {
  generated_at_utc: string;
  stale_after_utc: string;
  feeds: Record<FeedType, FeedStatus>;
}

/** Outcomes where data arrived but its content was wrong. Every other failure is "Not received". */
const INVALID_OUTCOMES = new Set(['invalid_protobuf', 'feed_metadata_error', 'feed_content_error']);

/** Throws when the document is not a readable v1 status; the caller treats that like a failed request. */
export function parseLatest(document: unknown): LatestStatus {
  const record = document as Partial<LatestStatus> & { schema_version?: unknown };
  if (!record || typeof record !== 'object' || record.schema_version !== 1) throw new Error('unknown schema_version');
  for (const key of ['generated_at_utc', 'stale_after_utc'] as const) {
    if (Number.isNaN(Date.parse(String(record[key])))) throw new Error(`${key} is not a timestamp`);
  }
  for (const { type } of FEEDS) {
    const feed = record.feeds?.[type];
    if (!feed || typeof feed !== 'object' || !('last_attempt' in feed)) throw new Error(`feed ${type} missing`);
  }
  return record as LatestStatus;
}

export type Tone = 'normal' | 'warning' | 'error' | 'muted';

export interface Cell {
  text: string;
  tone: Tone;
  /** The number behind the text, for the count-up; absent for words. */
  value?: number;
}

export interface StatusView {
  health: Cell & { note: string | null };
  metrics: { vehicles: Cell; trips: Cell; alerts: Cell; maxLag: Cell };
  rows: { type: FeedType; name: string; transitData: Cell; lastChecked: Cell; capturedSince: Cell; transitLag: Cell }[];
}

const ERROR: Cell = { text: 'Error', tone: 'error' };
const NA: Cell = { text: 'N/A', tone: 'muted' };

export function relativeTime(at: string, now: Date): string {
  const minutes = Math.floor((now.getTime() - Date.parse(at)) / 60_000);
  return minutes < 1 ? 'just now' : `${minutes} min ago`;
}

/** "3 min 03 s ago": to the second, seconds always two digits so the text keeps its width as it counts. */
export function elapsedToSecond(at: string, now: Date): string {
  const total = Math.max(0, Math.floor((now.getTime() - Date.parse(at)) / 1000));
  return `${Math.floor(total / 60)} min ${String(total % 60).padStart(2, '0')} s ago`;
}

export function isExpired(latest: LatestStatus, now: Date): boolean {
  return now.getTime() >= Date.parse(latest.stale_after_utc);
}

/** Received, and the audit was written: what the contract counts as a success. */
function succeeded(attempt: LastAttempt | null): boolean {
  return attempt?.outcome === 'success' && attempt.audit_status === 'written';
}

function transitData(attempt: LastAttempt | null): Cell {
  if (!attempt) return NA;
  if (attempt.outcome === 'success') return { text: 'Received', tone: 'normal' };
  if (attempt.outcome === null) return ERROR;
  return { text: INVALID_OUTCOMES.has(attempt.outcome) ? 'Invalid' : 'Not received', tone: 'error' };
}

function count(attempt: LastAttempt | null): Cell {
  if (!attempt) return NA;
  if (attempt.entity_count === null) return ERROR;
  return { text: String(attempt.entity_count), tone: 'normal', value: attempt.entity_count };
}

/** This feed's lag at its latest capture: feed_age_seconds, whole seconds. */
function lag(attempt: LastAttempt | null): Cell {
  if (!attempt) return NA;
  if (attempt.feed_age_seconds === null) return ERROR;
  const seconds = Math.round(attempt.feed_age_seconds);
  return { text: `${seconds} s`, tone: 'normal', value: seconds };
}

function maxLag(feeds: FeedStatus[]): Cell {
  const attempts = feeds.map((feed) => feed.last_attempt).filter((attempt) => attempt !== null);
  if (attempts.length === 0) return NA;
  const ages = attempts.map((attempt) => attempt.feed_age_seconds).filter((age) => age !== null);
  if (ages.length === 0) return ERROR;
  const seconds = Math.round(Math.max(...ages));
  return { text: `${seconds} s`, tone: 'normal', value: seconds };
}

/**
 * `latest` is the last file read successfully, or null when none has been. `refreshFailed` is set while the
 * latest request failed and the page is still showing that older file.
 */
export function buildView(latest: LatestStatus | null, now: Date, refreshFailed = false): StatusView {
  if (!latest) {
    return {
      health: { text: 'Unavailable', tone: 'error', note: 'Status not reachable' },
      metrics: { vehicles: ERROR, trips: ERROR, alerts: ERROR, maxLag: ERROR },
      rows: FEEDS.map(({ type, name }) => ({
        type, name, transitData: ERROR, lastChecked: ERROR, capturedSince: ERROR, transitLag: ERROR,
      })),
    };
  }

  const expired = isExpired(latest, now);
  const feeds = FEEDS.map(({ type }) => latest.feeds[type]);
  const rows = FEEDS.map(({ type, name }) => {
    const feed = latest.feeds[type];
    const attempt = feed.last_attempt;
    const lastChecked: Cell = attempt
      ? { text: relativeTime(attempt.cycle_completed_at_utc, now), tone: expired ? 'error' : 'normal' }
      : NA;
    // "Captured since": the age of the newest data held, from its feed header timestamp.
    let capturedSince: Cell;
    if (!attempt) capturedSince = NA;
    else if (!feed.last_success_feed_timestamp) capturedSince = ERROR;
    // Behind "Last checked" whenever the latest attempt did not count as a success.
    else capturedSince = {
      text: elapsedToSecond(feed.last_success_feed_timestamp, now),
      tone: succeeded(attempt) ? 'normal' : 'error',
    };
    return { type, name, transitData: transitData(attempt), lastChecked, capturedSince, transitLag: lag(attempt) };
  });

  const normal = feeds.every((feed) => succeeded(feed.last_attempt));
  let health: StatusView['health'] = normal
    ? { text: 'Normal', tone: 'normal', note: null }
    : { text: 'Partial data', tone: 'error', note: null };
  if (expired) health = { text: 'Unavailable', tone: 'error', note: 'Status file expired' };

  const view: StatusView = {
    health,
    metrics: {
      vehicles: count(latest.feeds.vehicle_positions.last_attempt),
      trips: count(latest.feeds.trip_updates.last_attempt),
      alerts: count(latest.feeds.service_alerts.last_attempt),
      maxLag: maxLag(feeds),
    },
    rows,
  };
  // An older file kept after a failed refresh: normal values turn orange until a new file arrives or it expires.
  return refreshFailed && !expired ? markKept(view) : view;
}

function warn(cell: Cell): Cell {
  return cell.tone === 'normal' ? { ...cell, tone: 'warning' } : cell;
}

function markKept(view: StatusView): StatusView {
  return {
    health: { ...warn(view.health), note: "Couldn't refresh, retrying" },
    metrics: {
      vehicles: warn(view.metrics.vehicles),
      trips: warn(view.metrics.trips),
      alerts: warn(view.metrics.alerts),
      maxLag: warn(view.metrics.maxLag),
    },
    rows: view.rows.map((row) => ({
      ...row,
      transitData: warn(row.transitData),
      lastChecked: warn(row.lastChecked),
      capturedSince: warn(row.capturedSince),
      transitLag: warn(row.transitLag),
    })),
  };
}

/** When the next file is expected: one capture interval after this one was written. */
export function nextDue(latest: LatestStatus): number {
  return Date.parse(latest.generated_at_utc) + CAPTURE_INTERVAL_MS;
}

/** How full the line under "Capture frequency" is: the share of this interval already gone, 0 to 1. */
export function captureProgress(latest: LatestStatus | null, now: Date): number {
  if (!latest || isExpired(latest, now)) return 0;
  const elapsed = now.getTime() - Date.parse(latest.generated_at_utc);
  return Math.min(1, Math.max(0, elapsed / CAPTURE_INTERVAL_MS));
}

/** "Now loading…" from ten seconds before the due time until a new file arrives or the old one expires. */
export function isLoading(latest: LatestStatus | null, now: Date): boolean {
  return latest !== null && !isExpired(latest, now) && now.getTime() >= nextDue(latest) - LOADING_LEAD_MS;
}

/**
 * Milliseconds until the next request. `missesSinceDue` counts requests since the due time that brought no newer
 * file, failed ones included; the caller resets it when a newer file arrives.
 */
export function nextFetchDelay(latest: LatestStatus | null, now: Date, missesSinceDue: number): number {
  if (latest && !isExpired(latest, now)) {
    const wait = nextDue(latest) - now.getTime();
    if (wait > 0) return wait;
  }
  return missesSinceDue < QUICK_RETRIES ? QUICK_RETRY_MS : SLOW_RETRY_MS;
}

/**
 * Milliseconds until the next whole second, when "Captured since" changes (feed timestamps are whole seconds).
 * Each tick recomputes from the system clock rather than adding a second, so a laptop that slept or a background
 * tab whose timers were held back shows the true age on the next tick.
 */
export function nextClockTick(now: Date): number {
  return CLOCK_TICK_MS - (now.getTime() % CLOCK_TICK_MS);
}

/** The count-up: 1.5 s, easeOutExpo, from the smallest number with as many digits, or from the previous value. */
export const COUNT_UP_MS = 1500;

export function easeOutExpo(t: number): number {
  return t >= 1 ? 1 : 1 - 2 ** (-10 * t);
}

export function countStart(target: number): number {
  const digits = String(Math.abs(Math.trunc(target))).length;
  return digits <= 1 ? 0 : 10 ** (digits - 1);
}

export function countFrame(from: number, to: number, elapsedMs: number): number {
  const t = Math.min(1, Math.max(0, elapsedMs / COUNT_UP_MS));
  return Math.round(from + (to - from) * easeOutExpo(t));
}
