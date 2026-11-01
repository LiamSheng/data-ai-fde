/**
 * Wires the status row and the details table to latest.json: requests, timers and the number motion.
 * Every decision about what to show comes from landing-status.ts; this file only applies it to the page.
 */
import {
  STATUS_URL, CAPTURE_INTERVAL_MS, LOADING_LEAD_MS, COUNT_UP_MS,
  buildView, parseLatest, captureProgress, isExpired, nextDue, nextFetchDelay, nextClockTick, countStart, countFrame,
  type Cell, type LatestStatus, type StatusView,
} from './landing-status';

/** The sweep draws across the figures in this time (then fades); each figure enters when the line reaches its centre. */
const SWEEP_DRAW_MS = 640;

export function startStatusBar(doc: Document, url = STATUS_URL): void {
  const found = {
    figures: doc.querySelector<HTMLElement>('[data-status-figures]'),
    capture: doc.querySelector<HTMLElement>('[data-status-capture]'),
    timer: doc.querySelector<HTMLElement>('[data-status-timer]'),
    health: doc.querySelector<HTMLElement>('[data-status-health]'),
    note: doc.querySelector<HTMLElement>('[data-status-note]'),
  };
  if (!found.figures || !found.capture || !found.timer || !found.health || !found.note) return;
  // Rebound as non-null: the checks above do not carry into the hoisted functions below.
  const figures: HTMLElement = found.figures;
  const capture: HTMLElement = found.capture;
  const timer: HTMLElement = found.timer;
  const health: HTMLElement = found.health;
  const note: HTMLElement = found.note;
  const metrics = [...doc.querySelectorAll<HTMLElement>('[data-status-metric]')];
  const items = metrics.map((el) => el.closest<HTMLElement>('.status-figure')!);
  const rows = [...doc.querySelectorAll<HTMLElement>('[data-status-row]')];
  const reduce = window.matchMedia('(prefers-reduced-motion: reduce)');

  let latest: LatestStatus | null = null;
  let refreshFailed = false;
  let misses = 0;
  let nextFetchAt = 0;
  let fetchTimer = 0, clockTimer = 0, loadingTimer = 0, expiryTimer = 0;
  // Figures count up the first time they are on screen, and from their old value on each new file.
  let seen = false;
  let loaded = false;
  const shown = new Map<HTMLElement, number>();
  const sweepTimers: number[] = [];
  // The figures wait unseen for their first sweep; without script or with reduced motion they show at once.
  if (!reduce.matches) figures.dataset.pending = '';

  function setCell(el: HTMLElement, cell: Cell) {
    el.dataset.tone = cell.tone;
    if (el.textContent !== cell.text && cell.value === undefined) el.textContent = cell.text;
  }

  /** The fade and rise from the Cobalt retries timeline. */
  function enter(el: HTMLElement) {
    el.classList.remove('is-entering');
    void el.offsetWidth;
    el.classList.add('is-entering');
  }

  function countUp(el: HTMLElement, cell: Cell, from: number) {
    const to = cell.value!;
    const unit = cell.text.slice(String(to).length);
    el.setAttribute('aria-label', cell.text);
    if (reduce.matches || from === to) { el.textContent = cell.text; shown.set(el, to); return; }
    const start = performance.now();
    const step = (now: number) => {
      const elapsed = now - start;
      el.textContent = `${countFrame(from, to, elapsed)}${unit}`;
      if (elapsed < COUNT_UP_MS) requestAnimationFrame(step); else el.textContent = cell.text;
    };
    requestAnimationFrame(step);
    shown.set(el, to);
  }

  /**
   * Writes the four figures. `animate` runs the sweep: the line crosses the panel and each figure enters as it passes,
   * starting its count-up only then. The first sweep brings in the whole figure, label included; later ones the number.
   */
  function renderFigures(view: StatusView, animate: boolean) {
    const cells = [view.metrics.vehicles, view.metrics.trips, view.metrics.alerts, view.metrics.maxLag];
    const first = figures.hasAttribute('data-pending');
    const sweep = animate && seen && !reduce.matches;
    if (sweep) sweepTimers.splice(0).forEach(clearTimeout);
    metrics.forEach((el, index) => {
      const cell = cells[index];
      setCell(el, cell);
      // The first sweep counts up from the smallest number with as many digits, later ones from the old value.
      const previous = sweep && first ? undefined : shown.get(el);
      if (cell.value === undefined) { el.removeAttribute('aria-label'); shown.delete(el); }
      else el.setAttribute('aria-label', cell.text);
      if (!sweep) {
        if (cell.value !== undefined) { el.textContent = cell.text; shown.set(el, cell.value); }
        return;
      }
      // Hold the old number (or nothing, the first time) until the line arrives.
      if (cell.value !== undefined && previous !== undefined) el.textContent = `${previous}${cell.text.slice(String(cell.value).length)}`;
      const at = ((index + 0.5) / metrics.length) * SWEEP_DRAW_MS;
      sweepTimers.push(window.setTimeout(() => {
        enter(first ? items[index] : el);
        if (cell.value !== undefined) countUp(el, cell, previous ?? countStart(cell.value));
      }, at));
    });
    if (!sweep) return;
    figures.removeAttribute('data-sweep');
    void figures.offsetWidth;
    figures.dataset.sweep = '';
    delete figures.dataset.pending;
    // The first time, the figures were unseen; keep each hidden until its own moment by entering from opacity 0.
    if (first) items.forEach((item) => { item.style.opacity = '0'; });
    sweepTimers.push(window.setTimeout(() => items.forEach((item) => item.style.removeProperty('opacity')), SWEEP_DRAW_MS + 400));
  }

  function renderTable(view: StatusView) {
    for (const row of rows) {
      const data = view.rows.find((r) => r.type === row.dataset.statusRow)!;
      for (const key of ['transitData', 'lastChecked', 'dataAge', 'transitLag'] as const) {
        const el = row.querySelector<HTMLElement>(`[data-cell="${key}"]`)!;
        el.dataset.tone = data[key].tone;
        if (el.textContent !== data[key].text) el.textContent = data[key].text;
      }
    }
  }

  function render(animate = false) {
    const now = new Date();
    const view = buildView(latest, now, refreshFailed);
    renderFigures(view, animate);
    setCell(health, view.health);
    note.textContent = view.health.note ?? '';
    note.dataset.tone = view.health.tone;
    renderTable(view);
    renderTimer(now);
  }

  /** The line under the frequency: the share of the interval since generated_at, then filling on its own. */
  function renderTimer(now: Date) {
    const progress = captureProgress(latest, now);
    timer.style.animation = 'none';
    timer.style.transform = `scaleX(${progress})`;
    if (!latest || progress >= 1 || progress === 0 && isExpired(latest, now)) return;
    void timer.offsetWidth;
    timer.style.animation = `status-timer ${CAPTURE_INTERVAL_MS}ms linear ${-progress * CAPTURE_INTERVAL_MS}ms forwards`;
  }

  function setLoading() {
    capture.toggleAttribute('data-loading', latest !== null && !isExpired(latest, new Date())
      && Date.now() >= nextDue(latest) - LOADING_LEAD_MS);
  }

  function scheduleFileTimers() {
    clearTimeout(loadingTimer);
    clearTimeout(expiryTimer);
    setLoading();
    if (!latest) return;
    const now = Date.now();
    const loadingAt = nextDue(latest) - LOADING_LEAD_MS;
    if (loadingAt > now) loadingTimer = window.setTimeout(setLoading, loadingAt - now);
    const expiresAt = Date.parse(latest.stale_after_utc);
    if (expiresAt > now) expiryTimer = window.setTimeout(() => { setLoading(); render(); }, expiresAt - now);
  }

  function scheduleClock() {
    clearTimeout(clockTimer);
    // Only the table moves with the clock; the figures and the timer line change with the file.
    clockTimer = window.setTimeout(() => {
      renderTable(buildView(latest, new Date(), refreshFailed));
      scheduleClock();
    }, nextClockTick(new Date()));
  }

  function scheduleFetch() {
    clearTimeout(fetchTimer);
    const delay = nextFetchDelay(latest, new Date(), misses);
    nextFetchAt = Date.now() + delay;
    fetchTimer = window.setTimeout(load, delay);
  }

  async function load() {
    clearTimeout(fetchTimer);
    const due = latest === null || Date.now() >= nextDue(latest);
    let newer = false;
    try {
      const response = await fetch(url, { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const next = parseLatest(await response.json());
      newer = latest === null || Date.parse(next.generated_at_utc) > Date.parse(latest.generated_at_utc);
      if (newer) latest = next;
      refreshFailed = false;
    } catch {
      // Keep the last file read; it turns orange, and red once it expires. Without one the page says not reachable.
      refreshFailed = latest !== null;
    }
    if (newer) misses = 0; else if (due) misses += 1;
    // The first answer, file or not, always gets its sweep; after that only a newer file does.
    render(newer || !loaded);
    loaded = true;
    scheduleFileTimers();
    scheduleClock();
    scheduleFetch();
  }

  // Timers stall while the laptop sleeps or the tab is hidden; on return, read the clock again and fetch if due.
  doc.addEventListener('visibilitychange', () => {
    if (doc.hidden) return;
    setLoading();
    render();
    scheduleClock();
    if (Date.now() >= nextFetchAt) void load();
  });

  const observer = new IntersectionObserver((entries) => {
    if (!entries.some((entry) => entry.isIntersecting) || seen) return;
    seen = true;
    observer.disconnect();
    // Before the first file, wait for it; its arrival runs the sweep. Otherwise sweep now.
    if (loaded) renderFigures(buildView(latest, new Date(), refreshFailed), true);
  });
  observer.observe(figures);

  void load();
}
