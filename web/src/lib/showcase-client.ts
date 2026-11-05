/**
 * Branch `showcase`: fills the "Live from the lakehouse" section from showcase.json, which a Databricks job
 * overwrites from the Bronze layer every 10 minutes. Until a valid file arrives the section stays hidden,
 * so a missing or broken file leaves the page exactly as it was.
 * Every value from the file goes in through textContent, never as HTML.
 */
export const SHOWCASE_URL = 'https://storage.googleapis.com/transit-realtime-showcase-26/v1/showcase.json';
const REFRESH_MS = 5 * 60_000;
const STALE_MS = 60 * 60_000;

interface RouteCount { route: string; vehicles: number }
interface RouteDelay { route: string; trips: number; avg_delay_seconds: number; late_share: number }
interface Alert { header: string | null; effect: string; routes: string[]; more_routes: number; since: string | null }
interface Feed { feed: string; attempts: number; failed: number; last_success_at: string | null; newest_data_at: string | null }

export interface Showcase {
  version: 1;
  generated_at_utc: string;
  vehicles: { snapshot_at: string; total: number; routes_active: number; top_routes: RouteCount[] } | null;
  delays: {
    window_minutes: number; late_threshold_seconds: number; samples: number; trips: number;
    late_share: number; median_delay_seconds: number | null; top_routes: RouteDelay[];
  } | null;
  alerts: { snapshot_at: string; total: number; by_effect: { effect: string; count: number }[]; recent: Alert[] } | null;
  health: { window_hours: number; feeds: Feed[]; quarantined_unresolved: number };
}

const FEED_NAMES: Record<string, string> = {
  vehicle_positions: 'Vehicle positions',
  trip_updates: 'Trip updates',
  service_alerts: 'Service alerts',
  gtfs_schedule: 'Static schedule',
};

export function parseShowcase(value: unknown): Showcase | null {
  const doc = value as Showcase;
  if (!doc || doc.version !== 1 || typeof doc.generated_at_utc !== 'string' || !doc.health) return null;
  return doc;
}

export function ago(iso: string | null, now: number): string {
  if (!iso) return '–';
  const minutes = Math.round((now - Date.parse(iso)) / 60_000);
  if (!Number.isFinite(minutes)) return '–';
  if (minutes < 1) return 'just now';
  if (minutes < 90) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  return hours < 48 ? `${hours} h ago` : `${Math.round(hours / 24)} d ago`;
}

export function delayText(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return '–';
  if (Math.abs(seconds) < 30) return 'on time';
  const minutes = (Math.abs(seconds) / 60).toFixed(1);
  return seconds > 0 ? `${minutes} min late` : `${minutes} min early`;
}

export function label(code: string): string {
  const words = code.toLowerCase().replace(/_/g, ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
}

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls?: string, text?: string) {
  const node = doc.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

function fill(root: HTMLElement, name: string, nodes: Node[]) {
  const slot = root.querySelector<HTMLElement>(`[data-sc="${name}"]`);
  if (slot) slot.replaceChildren(...nodes);
}

function text(root: HTMLElement, name: string, value: string) {
  const slot = root.querySelector<HTMLElement>(`[data-sc="${name}"]`);
  if (slot) slot.textContent = value;
}

function bars(doc: Document, items: { name: string; value: number; shown: string }[]): Node[] {
  const max = Math.max(1, ...items.map((item) => item.value));
  return items.map((item) => {
    const row = el(doc, 'li', 'sc-bar');
    row.append(el(doc, 'span', 'sc-bar-name', item.name), el(doc, 'span', 'sc-bar-value num', item.shown));
    const track = el(doc, 'span', 'sc-bar-track');
    const fillBar = el(doc, 'span', 'sc-bar-fill');
    fillBar.style.width = `${Math.max(2, (Math.max(0, item.value) / max) * 100)}%`;
    track.append(fillBar);
    row.append(track);
    return row;
  });
}

export function render(doc: Document, root: HTMLElement, data: Showcase, now = Date.now()) {
  const stale = now - Date.parse(data.generated_at_utc) > STALE_MS;
  text(root, 'generated', `Last computed ${ago(data.generated_at_utc, now)}${stale ? ' (the job may be paused)' : ''}.`);

  const v = data.vehicles;
  text(root, 'vehicles-total', v ? v.total.toLocaleString('en-CA') : '–');
  text(root, 'vehicles-note', v ? `on ${v.routes_active} routes · snapshot ${ago(v.snapshot_at, now)}` : 'No snapshot yet');
  fill(root, 'vehicles-routes', v ? bars(doc, v.top_routes.map((r) => ({
    name: r.route, value: r.vehicles, shown: String(r.vehicles),
  }))) : []);

  const d = data.delays;
  const hasDelays = !!d && d.samples > 0;
  text(root, 'delays-late', hasDelays ? `${Math.round(d!.late_share * 100)}%` : '–');
  text(root, 'delays-note', hasDelays
    ? `of ${d!.trips.toLocaleString('en-CA')} running trips were ${Math.round(d!.late_threshold_seconds / 60)}+ min late at their next stop in the last ${d!.window_minutes} min · median ${delayText(d!.median_delay_seconds)}`
    : 'Not enough predictions in the last hour');
  fill(root, 'delays-routes', hasDelays ? bars(doc, d!.top_routes.map((r) => ({
    name: r.route, value: r.avg_delay_seconds, shown: delayText(r.avg_delay_seconds),
  }))) : []);

  const a = data.alerts;
  text(root, 'alerts-total', a ? String(a.total) : '–');
  text(root, 'alerts-note', a ? `active alerts · snapshot ${ago(a.snapshot_at, now)}` : 'No snapshot yet');
  fill(root, 'alerts-effects', a ? a.by_effect.slice(0, 5).map((e) => el(doc, 'li', 'sc-chip', `${label(e.effect)} · ${e.count}`)) : []);
  fill(root, 'alerts-recent', a ? a.recent.map((alert) => {
    const item = el(doc, 'li', 'sc-alert');
    item.append(el(doc, 'p', 'sc-alert-head', alert.header ?? label(alert.effect)));
    const routes = alert.routes.join(', ') + (alert.more_routes ? ` +${alert.more_routes}` : '');
    const since = alert.since ? `since ${new Date(alert.since).toLocaleDateString('en-CA', { month: 'short', day: 'numeric' })}` : '';
    item.append(el(doc, 'p', 'sc-alert-meta', [label(alert.effect), routes && `Routes ${routes}`, since].filter(Boolean).join(' · ')));
    return item;
  }) : []);

  const h = data.health;
  fill(root, 'health-rows', h.feeds.map((f) => {
    const row = el(doc, 'tr');
    row.append(
      el(doc, 'th', undefined, FEED_NAMES[f.feed] ?? f.feed),
      el(doc, 'td', 'num', f.attempts.toLocaleString('en-CA')),
      el(doc, 'td', f.failed ? 'num sc-warn' : 'num', String(f.failed)),
      // Static captures carry no feed timestamp; their last successful capture stands in.
      el(doc, 'td', undefined, ago(f.newest_data_at ?? f.last_success_at, now)),
    );
    return row;
  }));
  text(root, 'health-note', `Last ${h.window_hours} h of captures landed in Bronze · ${h.quarantined_unresolved} file(s) quarantined for breaking the data contract.`);
}

export function startShowcase(doc: Document, url = SHOWCASE_URL): void {
  const root = doc.querySelector<HTMLElement>('[data-showcase]');
  if (!root) return;
  let latest: Showcase | null = null;

  async function refresh() {
    try {
      // The query string only defeats browser and CDN caches; GCS ignores it.
      const response = await fetch(`${url}?t=${Date.now()}`, { cache: 'no-store' });
      if (!response.ok) return;
      const data = parseShowcase(await response.json());
      if (!data) return;
      latest = data;
      render(doc, root!, data);
      root!.hidden = false;
    } catch {
      // Keep whatever is on screen; with nothing yet the section simply stays hidden.
    }
  }

  void refresh();
  window.setInterval(() => void refresh(), REFRESH_MS);
  // Relative times ("12 min ago") move on between refreshes.
  window.setInterval(() => { if (latest) render(doc, root, latest); }, 60_000);
}
