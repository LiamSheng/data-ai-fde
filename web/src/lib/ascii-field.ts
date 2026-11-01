/**
 * The ASCII field: strands of traffic run left to right across the band that holds the
 * workspace, dense under the header and thinning toward the bottom of the panels. Each strand
 * is a dotted line with a few dense bodies travelling along it, most trailing a line of
 * pipeline code, SQL or an assistant step. The map covers the field; it shows
 * on the paper around and between the panels, and under the glass of the status row, the details and the chat, which blurs it.
 * One canvas on an 8 by 16 cell; colours come from the field greys in global.css, never literals.
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
// Strands start this many rows apart, give or take, and up to STRAND_FADE rows further apart at the bottom;
// a body spreads at most this many rows off its strand.
const STRAND_MIN = 1;
const STRAND_SPREAD = 2;
const STRAND_FADE = 13;
const BODY_ROWS = 2;

// What strands carry: the pipeline behind the map (tables and columns from the models in data-platform/)
// and what the assistant does with it, interleaved so neighbouring strands differ.
const TEXTS = [
  'spark.readStream.table("bronze.vehicle_positions")',
  "select route_id, avg(arrival_delay) from gold.fct_trip_delay_sample group by route_id",
  'Thinking...',
  '.withWatermark("event_ts", "10 minutes")',
  "where trip_schedule_relationship in ('SCHEDULED', 'DUPLICATED')",
  'tool: genie.ask("average delay by route")',
  '.groupBy(F.window("event_ts", "5 minutes"), "route_id")',
  'row_number() over (partition by payload_sha256, entity_index order by stop_sequence)',
  'reading gold.route_delay_summary',
  '.writeStream.toTable("silver.stg_trip_updates")',
  'timestampdiff(SECOND, feed_timestamp, arrival_time) as prediction_lead_seconds',
  'dbt run --select gold.route_delay_summary',
  'drafting answer...',
];

interface Box { left: number; top: number; right: number; bottom: number }
// `code` marks the letters of a snippet or a step: bold and opaque, over the lighter symbols.
type Cell = [glyph: string, tone: string, code?: boolean] | undefined;
interface Body { phase: number; speed: number; length: number; rows: number; text?: string }
// `fade` runs from 0 under the header to 1 at the bottom of the panels.
// `frame` is where each body sits this frame and where its text starts, undefined when the text would cross another.
interface Strand { y: number; fade: number; bodies: Body[]; frame: { centre: number; textStart?: number }[] }

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

export function startAsciiField(canvas: HTMLCanvasElement, workspace: HTMLElement): () => void {
  const ctx = canvas.getContext('2d');
  // The map first: it is the one panel that covers the field. The status panels are glass like the details and the chat.
  const panels = [...['.map-panel', '.detail-panel', '.chat-panel'].map((selector) => workspace.querySelector<HTMLElement>(selector)),
    ...workspace.querySelectorAll<HTMLElement>('.status-panel')];
  if (!ctx || panels.some((panel) => !panel)) return () => {};

  const tokens = getComputedStyle(document.documentElement);
  const token = (name: string) => tokens.getPropertyValue(name).trim();
  // One grey per ramp step, never lighter than field-1; blue is kept for the data in the panels.
  const paperTones = ['--color-field-1', '--color-field-1', '--color-field-2', '--color-field-2', '--color-field-2',
    '--color-field-3', '--color-field-3', '--color-field-3', '--color-field-4'].map(token);
  const glyph4 = token('--color-field-3');
  const font = `400 12px ${token('--font-outlier')}`;
  const codeFont = `700 12px ${token('--font-outlier')}`;
  const reduce = window.matchMedia('(prefers-reduced-motion: reduce)');

  let width = 0, height = 0, bottom = 0;
  let boxes: Box[] = [];
  // The map covers the field; the details and the chat are glass, so the field runs on under them.
  let covered: Box[] = [];
  let strands: Strand[] = [];
  let pointer: { x: number; y: number } | undefined;

  const relative = (el: Element, origin: DOMRect): Box => {
    const r = el.getBoundingClientRect();
    return { left: r.left - origin.left, top: r.top - origin.top, right: r.right - origin.left, bottom: r.bottom - origin.top };
  };

  /** Strands on whole rows, a few apart, plus one through the middle of every gap between stacked panels. */
  function layStrands() {
    const rows = new Set<number>();
    const fadeAt = (row: number) => Math.min(1, (row * CELL_H) / bottom);
    for (let row = 1; row * CELL_H < bottom; row += STRAND_MIN + Math.floor(hash(row, 7) * STRAND_SPREAD + fadeAt(row) * STRAND_FADE)) rows.add(row);
    const sorted = [...boxes].sort((a, b) => a.top - b.top);
    for (let k = 1; k < sorted.length; k++) {
      const gap = sorted[k].top - sorted[k - 1].bottom;
      if (gap < CELL_H) continue;
      const row = Math.floor((sorted[k - 1].bottom + gap / 2) / CELL_H);
      for (const near of [row - 2, row - 1, row + 1, row + 2]) rows.delete(near);
      rows.add(row);
    }
    let text = 0;
    strands = [...rows].sort((a, b) => a - b).map((row) => {
      const fade = fadeAt(row);
      // Lower strands carry fewer, shorter and thinner bodies.
      return {
        y: row * CELL_H + CELL_H / 2,
        fade,
        frame: [],
        bodies: Array.from({ length: 1 + Math.floor((1 + hash(row, 3) * 3) * (1 - 0.8 * fade)) }, (_, b) => ({
          phase: hash(row, b + 11) * 4000,
          speed: 28 + hash(row, b + 19) * 46,
          length: (110 + hash(row, b + 23) * 300) * (1 - 0.5 * fade),
          rows: Math.floor(hash(row, b + 29) * (BODY_ROWS + 1) * (1 - 0.7 * fade)),
          text: hash(row, b + 31) < 0.8 ? TEXTS[text++ % TEXTS.length] : undefined,
        })),
      };
    });
  }

  function measure() {
    const origin = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    width = origin.width;
    height = origin.height;
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    ctx!.setTransform(dpr, 0, 0, dpr, 0, 0);
    boxes = panels.map((panel) => relative(panel!, origin));
    covered = boxes.slice(0, 1);
    // The field ends one row below the lowest panel.
    bottom = Math.min(height, Math.max(...boxes.map((box) => box.bottom)) + CELL_H);
    layStrands();
  }

  const ramp = (v: number, cap = RAMP.length - 1): Cell => {
    const index = Math.min(cap, Math.floor(Math.max(0, Math.min(1, v)) * RAMP.length));
    return index < 1 ? undefined : [RAMP[index], paperTones[index - 1]];
  };

  /** One cell against the strands within reach of its row: the strongest body wins; a body's text rides just behind it. */
  function strandCell(x: number, y: number, near: Strand[], t: number): Cell {
    let v = 0;
    for (const strand of near) {
      let text: string | undefined;
      const dy = Math.round((y - strand.y) / CELL_H);
      // The strand itself: a sparse dotted line across the band.
      if (dy === 0 && noise(x * 0.04 - t * 0.6, strand.y) > 0.3 + 0.45 * strand.fade) v = Math.max(v, 0.12);
      strand.bodies.forEach((body, b) => {
        if (Math.abs(dy) > body.rows) return;
        const { centre, textStart } = strand.frame[b];
        if (dy === 0 && body.text && textStart !== undefined) {
          const col = Math.floor((x - textStart) / CELL_W);
          if (col >= 0 && col < body.text.length) text = body.text[col];
        }
        // Rows off the strand are shorter, so a body tapers like a stroke.
        const dx = ((x - centre) / body.length) * (1 + 0.7 * Math.abs(dy));
        if (Math.abs(dx) > 1.4) return;
        const along = Math.exp(-3 * dx * dx) + (dx < 0 && dy === 0 ? 0.25 : 0);
        const across = Math.exp(-(dy * dy) / ((body.rows + 0.5) ** 2));
        v = Math.max(v, along * across * (0.75 + 0.35 * noise(x * 0.1 - t * 2, strand.y + dy)));
      });
      if (text !== undefined) return text === ' ' ? undefined : [text, glyph4, true];
    }
    return ramp(v);
  }

  /** Places every body for this frame; a body's text is left out while it would run into text already placed on its strand. */
  function placeBodies(t: number) {
    for (const strand of strands) {
      const taken: [number, number][] = [];
      strand.frame = strand.bodies.map((body) => {
        const textWidth = (body.text?.length ?? 0) * CELL_W;
        const centre = ((body.phase + t * body.speed) % (width + 2 * body.length + textWidth)) - body.length;
        if (!body.text) return { centre };
        const start = centre - body.length - textWidth;
        // The span runs to the body's centre, so text never sits on another body either.
        if (taken.some(([a, b]) => start < b && centre > a)) return { centre };
        taken.push([start, centre]);
        return { centre, textStart: start };
      });
    }
  }

  function draw(t: number) {
    ctx!.clearRect(0, 0, width, height);
    ctx!.textBaseline = 'middle';
    placeBodies(t);
    const symbols: [number, number, string, string][] = [];
    const code: [number, number, string, string][] = [];
    for (let y = CELL_H / 2; y < bottom; y += CELL_H) {
      const near = strands.filter((strand) => Math.abs(strand.y - y) <= BODY_ROWS * CELL_H);
      for (let x = CELL_W / 2; x < width; x += CELL_W) {
        if (covered.some((box) => inside(box, x, y))) continue;
        let cell = near.length ? strandCell(x, y, near, t) : undefined;
        if (pointer && !cell?.[2]) {
          const dist = Math.hypot(x - pointer.x, y - pointer.y);
          // The lens gathers the ramp under the pointer; code and steps keep their letters.
          if (dist < LENS_PX) cell = ramp((cell ? RAMP.indexOf(cell[0]) / RAMP.length : 0) + 0.35 * (1 - dist / LENS_PX));
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
