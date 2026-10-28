/**
 * The ASCII field: traffic enters across the whole left gutter and narrows onto the left column,
 * runs through the gaps between the three panels as pipeline code, and past the Chat panel
 * narrows again, as the model's steps, to one dense band at the page edge.
 * The panels cover the field; it draws only on the paper around and between them.
 * One canvas on an 8 by 16 cell; colours come from the Cobalt tokens, never literals.
 */
const CELL_W = 8;
const CELL_H = 16;
const RAMP = ' .·:;+*#%@';
const FRAME_MS = 1000 / 30;
// The symbols sit back behind the code.
const SYMBOL_ALPHA = 0.6;
const LENS_PX = 72;
// The reduced-motion still is drawn at this moment of the loop.
const STILL_T = 14;
// At the page edge the right funnel is this share of the chat's height; the left one reaches this far past the workspace.
const OUT_BAND = 0.3;
const IN_REACH = 0.25;

// The gaps carry the transformations behind the map, named after the real tables in data-platform/.
const CODE = [
  'spark.readStream.table("bronze.vehicle_positions").withWatermark("event_ts", "10 minutes")',
  '.groupBy(F.window("event_ts", "5 minutes"), "route_id").agg(F.avg("delay_s").alias("avg_delay"))',
  '.writeStream.toTable("silver.stg_trip_updates")',
  'dbt run --select gold.route_delay_summary',
];
// Past the chat: what the assistant does with them.
const STEPS = [
  'Thinking...',
  'tool: genie.ask("average delay by route")',
  'reading gold.route_delay_summary',
  'Thinking...',
  'checking the snapshot time',
  'drafting answer...',
];

interface Box { left: number; top: number; right: number; bottom: number }
// `code` marks the letters of a snippet or a step: bold and opaque, over the lighter symbols.
type Cell = [glyph: string, tone: string, code?: boolean] | undefined;

const smoothstep = (lo: number, hi: number, x: number) => {
  const t = Math.min(1, Math.max(0, (x - lo) / (hi - lo)));
  return t * t * (3 - 2 * t);
};

const hash = (x: number, y: number) => {
  const s = Math.sin(x * 127.1 + y * 311.7) * 43758.5453;
  return s - Math.floor(s);
};

/** Value noise in [0, 1]. */
function noise(x: number, y: number): number {
  const xi = Math.floor(x), yi = Math.floor(y);
  const xf = x - xi, yf = y - yi;
  const u = xf * xf * (3 - 2 * xf), v = yf * yf * (3 - 2 * yf);
  const top = hash(xi, yi) + (hash(xi + 1, yi) - hash(xi, yi)) * u;
  const bottom = hash(xi, yi + 1) + (hash(xi + 1, yi + 1) - hash(xi, yi + 1)) * u;
  return top + (bottom - top) * v;
}

const inside = (box: Box, x: number, y: number) => x >= box.left && x < box.right && y >= box.top && y < box.bottom;

// Half-open, so a centre on a cell boundary still lands on exactly one row.
const onRow = (y: number, centre: number) => centre >= y - CELL_H / 2 && centre < y + CELL_H / 2;

/** The character of `text` at cell column `col`, scrolling with the flow; undefined in the space between repeats. */
const scroll = (text: string, col: number, t: number, speed: number) => {
  const span = text.length + 6;
  const i = (((Math.floor(col - t * speed)) % span) + span) % span;
  return i < text.length ? text[i] : undefined;
};

export function startAsciiField(canvas: HTMLCanvasElement, workspace: HTMLElement): () => void {
  const ctx = canvas.getContext('2d');
  const panels = ['.map-panel', '.detail-panel', '.chat-panel'].map((selector) => workspace.querySelector<HTMLElement>(selector));
  if (!ctx || panels.some((panel) => !panel)) return () => {};

  const tokens = getComputedStyle(document.documentElement);
  const token = (name: string) => tokens.getPropertyValue(name).trim();
  // One tone per ramp step; the field never goes lighter than glyph-2.
  const paperTones = ['--color-glyph-2', '--color-glyph-2', '--color-glyph-3', '--color-glyph-3', '--color-glyph-3',
    '--color-glyph-4', '--color-glyph-4', '--color-glyph-4', '--color-accent-deep'].map(token);
  const [glyph3, glyph4, deep] = ['--color-glyph-3', '--color-glyph-4', '--color-accent-deep'].map(token);
  const font = `400 12px ${token('--font-outlier')}`;
  const codeFont = `700 12px ${token('--font-outlier')}`;
  const reduce = window.matchMedia('(prefers-reduced-motion: reduce)');

  let width = 0, height = 0;
  let map: Box, detail: Box, chat: Box;
  let vertical = false;
  let pointer: { x: number; y: number } | undefined;

  const relative = (el: Element, origin: DOMRect): Box => {
    const r = el.getBoundingClientRect();
    return { left: r.left - origin.left, top: r.top - origin.top, right: r.right - origin.left, bottom: r.bottom - origin.top };
  };

  function measure() {
    const origin = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    width = origin.width;
    height = origin.height;
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    ctx!.setTransform(dpr, 0, 0, dpr, 0, 0);
    [map, detail, chat] = panels.map((panel) => relative(panel!, origin));
    // Stacked panels (narrow screens) turn the flow from left-to-right into top-to-bottom.
    vertical = chat.top >= detail.bottom - 1;
  }

  const ramp = (v: number, cap = RAMP.length - 1): Cell => {
    const index = Math.min(cap, Math.floor(Math.max(0, Math.min(1, v)) * RAMP.length));
    return index < 1 ? undefined : [RAMP[index], paperTones[index - 1]];
  };

  /**
   * A funnel in flow space (`a` along the flow, `b` across it): from [from0, from1] at a0 to [to0, to1] at a1,
   * filled edge to edge, denser where it is narrow; `hot` is a line across the flow that runs denser still.
   */
  function funnel(a: number, b: number, a0: number, a1: number, from: [number, number], to: [number, number], t: number, hot?: number): Cell {
    const e = smoothstep(0, 1, (a - a0) / (a1 - a0));
    const top = from[0] + (to[0] - from[0]) * e;
    const bottom = from[1] + (to[1] - from[1]) * e;
    if (b < top || b >= bottom) return undefined;
    const s = (b - top) / (bottom - top);
    // Streaks follow the funnel's walls because they are sampled on `s`, not on `b`.
    const streak = smoothstep(0.25, 0.85, noise(a * 0.025 - t * 1.3, s * 22));
    let v = (0.18 + 0.3 * e) + (0.3 + 0.25 * e) * streak;
    // Packets run along the streaks across the whole funnel, so the wide part moves as much as the narrow one.
    const packet = noise(a * 0.07 - t * 3.2, s * 22 + 50);
    if (packet > 0.74) v += 0.5 * smoothstep(0.74, 0.9, packet);
    if (hot !== undefined) {
      const h = (hot - top) / (bottom - top);
      v += 0.6 * e * Math.exp(-(((s - h) / 0.07) ** 2));
    }
    return ramp(v);
  }

  /** A gap between panels one or two rows deep: a row of pipeline code over a row of packets. */
  function channel(a: number, b: number, top: number, bottom: number, t: number, phase: number): Cell {
    const rows = Math.max(1, Math.round((bottom - top) / CELL_H));
    const row = Math.floor((b - top) / CELL_H);
    if (row === 0 || rows === 1) {
      const char = scroll(CODE[phase % CODE.length], a / CELL_W, t, 8);
      if (char !== undefined) return char === ' ' ? undefined : [char, glyph4, true];
    }
    const packet = noise(a * 0.03 - t * 2.2 + phase * 13, row * 3 + phase);
    return packet > 0.72 ? ['#', deep] : ramp(0.35 + 0.5 * packet, 7);
  }

  /** A gap the width of a few cells running across the flow: packets converge on `mid`. */
  function column(b: number, x: number, x0: number, x1: number, mid: number, t: number): Cell {
    const packet = noise(Math.abs(b - mid) * 0.03 + t * 2, x * 0.1);
    if (Math.abs(x - (x0 + x1) / 2) < CELL_W / 2) return packet > 0.7 ? ['#', deep] : [':', glyph4];
    return ramp(0.2 + 0.5 * packet, 6);
  }

  /** Past the chat: the steps leave as straight rows while the funnel is wide, then the funnel takes them in. */
  function steps(a: number, b: number, a0: number, a1: number, lineTop: number, lineBottom: number, t: number): Cell {
    if ((a - a0) / (a1 - a0) > 0.45) return undefined;
    for (let k = 0; k < STEPS.length; k++) {
      const line = lineTop + ((k + 0.5) / STEPS.length) * (lineBottom - lineTop);
      if (!onRow(b, line)) continue;
      const char = scroll(STEPS[k], (a - a0) / CELL_W, t, 5 + k);
      if (char !== undefined) return char === ' ' ? undefined : [char, k % 3 === 0 ? glyph4 : glyph3, true];
    }
    return undefined;
  }

  function horizontalCell(x: number, y: number, t: number): Cell {
    const gapMid = (map.bottom + detail.top) / 2;
    if (x < map.left) {
      const reach = (detail.bottom - map.top) * IN_REACH;
      return funnel(x, y, 0, map.left, [Math.max(0, map.top - reach), Math.min(height, detail.bottom + reach)], [map.top, detail.bottom], t, gapMid);
    }
    if (x < map.right) return y >= map.bottom && y < detail.top ? channel(x, y, map.bottom, detail.top, t, 0) : undefined;
    if (x < chat.left) return y >= map.top && y < detail.bottom ? column(y, x, map.right, chat.left, gapMid, t) : undefined;
    if (x < chat.right) return undefined;
    const mid = (chat.top + chat.bottom) / 2;
    const band = ((chat.bottom - chat.top) * OUT_BAND) / 2;
    const end = width;
    return steps(x, y, chat.right, end, chat.top, chat.bottom, t)
      ?? funnel(x, y, chat.right, end, [chat.top, chat.bottom], [mid - band, mid + band], t, mid);
  }

  function verticalCell(x: number, y: number, t: number): Cell {
    if (y < map.top) return funnel(y, x, 0, map.top, [0, width], [map.left, map.right], t);
    if (y >= map.bottom && y < detail.top) return channel(x, y, map.bottom, detail.top, t, 0);
    if (y >= detail.bottom && y < chat.top) return channel(x, y, detail.bottom, chat.top, t, 2);
    if (y < chat.bottom) {
      if (x < map.left) return column(y, x, 0, map.left, (map.top + chat.bottom) / 2, t);
      if (x >= map.right) return column(y, x, map.right, width, (map.top + chat.bottom) / 2, t);
      return undefined;
    }
    const mid = (chat.left + chat.right) / 2;
    const band = ((chat.right - chat.left) * OUT_BAND) / 2;
    return funnel(y, x, chat.bottom, height, [0, width], [mid - band, mid + band], t, mid);
  }

  function draw(t: number) {
    ctx!.clearRect(0, 0, width, height);
    ctx!.textBaseline = 'middle';
    const symbols: [number, number, string, string][] = [];
    const code: [number, number, string, string][] = [];
    for (let y = CELL_H / 2; y < height; y += CELL_H) {
      for (let x = CELL_W / 2; x < width; x += CELL_W) {
        if (inside(map, x, y) || inside(detail, x, y) || inside(chat, x, y)) continue;
        let cell = vertical ? verticalCell(x, y, t) : horizontalCell(x, y, t);
        if (pointer) {
          const dist = Math.hypot(x - pointer.x, y - pointer.y);
          // The lens gathers the ramp under the pointer; code and steps keep their letters.
          if (dist < LENS_PX && !cell?.[2]) {
            const index = cell ? RAMP.indexOf(cell[0]) : 0;
            if (cell || vertical || x < map.left || x >= chat.right) cell = ramp(index / RAMP.length + 0.35 * (1 - dist / LENS_PX));
          }
        }
        if (cell) (cell[2] ? code : symbols).push([x - CELL_W / 2, y, cell[0], cell[1]]);
      }
    }
    // Two passes, so the font changes twice a frame rather than per cell.
    for (const [face, alpha, cells] of [[font, SYMBOL_ALPHA, symbols], [codeFont, 1, code]] as const) {
      ctx!.font = face;
      ctx!.globalAlpha = alpha;
      for (const [x, y, glyph, tone] of cells) {
        ctx!.fillStyle = tone;
        ctx!.fillText(glyph, x, y);
      }
    }
    ctx!.globalAlpha = 1;
  }

  let raf = 0;
  let last = 0;
  let onScreen = true;
  const tick = (now: number) => {
    raf = requestAnimationFrame(tick);
    if (now - last < FRAME_MS) return;
    last = now;
    draw(now / 1000);
  };
  const stop = () => { cancelAnimationFrame(raf); raf = 0; };
  const start = () => {
    if (reduce.matches) { stop(); draw(STILL_T); return; }
    if (!raf && onScreen && !document.hidden) raf = requestAnimationFrame(tick);
  };

  const resize = new ResizeObserver(() => { measure(); if (!raf) draw(reduce.matches ? STILL_T : performance.now() / 1000); });
  resize.observe(canvas);
  resize.observe(workspace);
  const visibility = new IntersectionObserver(([entry]) => {
    onScreen = entry.isIntersecting;
    if (onScreen) start(); else stop();
  });
  visibility.observe(canvas);
  const onVisibility = () => (document.hidden ? stop() : start());
  const onPointer = (event: PointerEvent) => {
    if (reduce.matches) return;
    const origin = canvas.getBoundingClientRect();
    pointer = { x: event.clientX - origin.left, y: event.clientY - origin.top };
  };
  const onLeave = () => { pointer = undefined; };
  document.addEventListener('visibilitychange', onVisibility);
  window.addEventListener('pointermove', onPointer, { passive: true });
  document.documentElement.addEventListener('pointerleave', onLeave);
  reduce.addEventListener('change', start);

  measure();
  canvas.dataset.live = '';
  // Repaint once the mono face arrives so the first frames are not in the fallback.
  Promise.all([font, codeFont].map((face) => document.fonts?.load(face))).then(() => { if (!raf) draw(reduce.matches ? STILL_T : performance.now() / 1000); }, () => {});
  start();

  return () => {
    stop();
    resize.disconnect();
    visibility.disconnect();
    document.removeEventListener('visibilitychange', onVisibility);
    window.removeEventListener('pointermove', onPointer);
    document.documentElement.removeEventListener('pointerleave', onLeave);
    reduce.removeEventListener('change', start);
    delete canvas.dataset.live;
  };
}
