import test from 'node:test';
import assert from 'node:assert/strict';
import { initializeMapPanel } from '../src/lib/map-client.ts';

// A controlled SDK boundary: no requests leave this fixture.
function fixture({ constructorThrows = false, constructorAuthFailure = false } = {}) {
  let sequence = 0;
  let constructors = 0;
  const timers = new Map();
  const windowListeners = new Map();
  const scripts = [];
  const maps = [];
  const win = {
    setTimeout(fn) { const id = ++sequence; timers.set(id, fn); return id; },
    clearTimeout(id) { timers.delete(id); },
    addEventListener(event, fn) {
      const listeners = windowListeners.get(event) ?? new Set();
      listeners.add(fn); windowListeners.set(event, listeners);
    },
    removeEventListener(event, fn) { windowListeners.get(event)?.delete(fn); },
  };
  const doc = {
    head: { append(script) { scripts.push(script); } },
    createElement(tag) {
      assert.equal(tag, 'script');
      const events = new Map();
      return {
        dataset: {}, src: '', async: false, events,
        addEventListener(event, fn) { events.set(event, fn); },
        removeEventListener(event, fn) { if (events.get(event) === fn) events.delete(event); },
      };
    },
  };
  class FakeMap {
    constructor(canvas, options) {
      constructors++;
      if (constructorThrows) throw new Error('fixture constructor error');
      this.canvas = canvas; this.options = options; this.listeners = new Set();
      maps.push(this);
      if (constructorAuthFailure) win.gm_authFailure();
    }
    addListener(event, fn) {
      assert.equal(event, 'tilesloaded');
      this.listeners.add(fn);
      return { remove: () => this.listeners.delete(fn) };
    }
    tiles() { for (const fn of [...this.listeners]) fn(); }
  }
  const root = () => {
    const elements = new Map([
      ['[data-map-canvas]', {}],
    ]);
    return { dataset: {}, isConnected: true, elements, querySelector: (selector) => elements.get(selector) ?? null };
  };
  const callback = () => new URL(scripts[0].src).searchParams.get('callback');
  const ready = () => { win.google = { maps: { Map: FakeMap } }; win[callback()](); };
  const timeout = () => {
    assert.equal(timers.size, 1);
    const [id, fn] = timers.entries().next().value;
    timers.delete(id); fn();
  };
  const pagehide = (persisted = false) => {
    for (const fn of [...(windowListeners.get('pagehide') ?? [])]) fn({ persisted });
  };
  return { environment: { window: win, document: doc }, win, scripts, maps, timers, root, ready, callback, timeout, pagehide, constructors: () => constructors };
}

const flush = async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); };
const KEY = 'fixture-only-not-a-real-key';

test('server import and missing/blank keys require no SDK request', () => {
  for (const key of [undefined, '', '   ']) {
    const f = fixture();
    const root = f.root();
    const c = initializeMapPanel(root, key, f.environment);
    assert.equal(c.state, 'waiting');
    assert.equal(root.dataset.mapState, 'waiting');
    assert.equal(f.scripts.length, 0);
    assert.equal(f.constructors(), 0);
    assert.equal(f.timers.size, 0);
  }
});

test('same root is initialized once and waits for actual tile readiness', async () => {
  const f = fixture(); const root = f.root();
  const c = initializeMapPanel(root, ` ${KEY} `, f.environment);
  assert.equal(initializeMapPanel(root, KEY, f.environment), c);
  assert.equal(c.state, 'loading');
  const parameters = new URL(f.scripts[0].src).searchParams;
  assert.equal(parameters.get('key'), KEY);
  assert.equal(parameters.get('loading'), 'async');
  assert.equal(parameters.get('v'), 'quarterly');
  f.ready(); await flush();
  assert.equal(c.state, 'loading');
  assert.equal(f.constructors(), 1);
  assert.deepEqual(c.map.options.center, { lat: 49.1044, lng: -122.6604 });
  f.maps[0].tiles();
  assert.equal(c.state, 'loaded');
  assert.equal(f.timers.size, 0);
  assert.equal(f.maps[0].listeners.size, 0);
});

test('two map panels share one document SDK request, each creates only its own map', async () => {
  const f = fixture();
  const first = initializeMapPanel(f.root(), KEY, f.environment);
  const second = initializeMapPanel(f.root(), KEY, f.environment);
  assert.equal(f.scripts.length, 1);
  f.ready(); await flush();
  assert.equal(f.constructors(), 2);
  for (const map of f.maps) map.tiles();
  assert.equal(first.state, 'loaded'); assert.equal(second.state, 'loaded');
});

test('SDK script error fails safely and a late callback cannot construct a map', async () => {
  const f = fixture(); const root = f.root();
  const c = initializeMapPanel(root, KEY, f.environment);
  f.scripts[0].events.get('error')(); await flush();
  assert.equal(c.state, 'failed');
  assert.equal(f.timers.size, 0);
  assert.equal(f.scripts[0].events.size, 0);
  f.ready(); await flush();
  assert.equal(f.constructors(), 0); assert.equal(c.state, 'failed');
});

test('SDK timeout is terminal, and does not retry the request', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.timeout(); await flush();
  f.ready(); await flush();
  assert.equal(c.state, 'failed'); assert.equal(f.constructors(), 0);
  assert.equal(f.scripts.length, 1); assert.equal(f.timers.size, 0);
});

test('readiness callback without a usable SDK fails', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.win[f.callback()](); await flush();
  assert.equal(c.state, 'failed'); assert.equal(f.constructors(), 0);
});

test('authentication before readiness fails, preserves the previous callback, blocks construction', async () => {
  const f = fixture(); let previousCalls = 0;
  const previous = () => { previousCalls++; };
  f.win.gm_authFailure = previous;
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.win.gm_authFailure();
  assert.equal(f.timers.size, 0);
  f.ready(); await flush();
  assert.equal(c.state, 'failed'); assert.equal(f.constructors(), 0);
  assert.equal(previousCalls, 1);
  f.pagehide();
  assert.equal(f.win.gm_authFailure, previous);
  assert.equal(f.timers.size, 0);
});

test('authentication after readiness but before tiles prevents late success', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush();
  const lateTiles = [...f.maps[0].listeners][0];
  f.win.gm_authFailure(); lateTiles();
  assert.equal(c.state, 'failed'); assert.equal(f.timers.size, 0);
});

test('authentication failure after loaded remains observable', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush(); f.maps[0].tiles();
  assert.equal(c.state, 'loaded');
  f.win.gm_authFailure();
  assert.equal(c.state, 'failed');
});

test('map constructor failure is contained', async () => {
  const f = fixture({ constructorThrows: true });
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush();
  assert.equal(c.state, 'failed'); assert.equal(f.timers.size, 0);
});

test('authentication during map construction cannot start a tile timer or revive success', async () => {
  const f = fixture({ constructorAuthFailure: true });
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush();
  assert.equal(c.state, 'failed'); assert.equal(f.timers.size, 0);
  assert.equal(f.maps[0].listeners.size, 0);
});

test('tile timeout clears listeners and ignores an already queued late event', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush();
  const lateTiles = [...f.maps[0].listeners][0];
  f.timeout(); lateTiles();
  assert.equal(c.state, 'failed');
  assert.equal(f.maps[0].listeners.size, 0); assert.equal(f.timers.size, 0);
});

test('a disconnected root is not changed by late SDK readiness', async () => {
  const f = fixture(); const root = f.root();
  const c = initializeMapPanel(root, KEY, f.environment);
  root.isConnected = false; f.ready(); await flush();
  assert.equal(f.constructors(), 0); assert.equal(c.state, 'loading');
  c.destroy(); assert.equal(f.timers.size, 0);
});

test('page unload releases timers, callbacks and the owned map', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush();
  const name = f.callback(); f.pagehide();
  assert.equal(c.map, undefined); assert.equal(f.timers.size, 0);
  assert.equal(f.maps[0].listeners.size, 0);
  assert.equal(f.win.gm_authFailure, undefined); assert.equal(f.win[name], undefined);
});

test('unload during SDK request prevents subsequent construction', async () => {
  const f = fixture();
  initializeMapPanel(f.root(), KEY, f.environment);
  f.pagehide(); await flush();
  assert.equal(f.timers.size, 0); assert.equal(f.constructors(), 0);
});

test('Back/Forward cache preserves the map and late authentication handling', async () => {
  const f = fixture();
  const c = initializeMapPanel(f.root(), KEY, f.environment);
  f.ready(); await flush(); f.maps[0].tiles();
  const map = c.map;
  f.pagehide(true);
  assert.equal(c.map, map); assert.equal(c.state, 'loaded');
  f.win.gm_authFailure();
  assert.equal(c.state, 'failed');
  f.pagehide(); assert.equal(c.map, undefined);
});

test('a map panel with only a canvas completes waiting, loading and loaded', async () => {
  const f = fixture(); const root = f.root();
  const c = initializeMapPanel(root, KEY, f.environment);
  assert.equal(root.dataset.mapState, 'loading');
  f.ready(); await flush();
  f.maps[0].tiles();
  assert.equal(c.state, 'loaded');
  assert.equal(root.dataset.mapState, 'loaded');
});

test('an incomplete component fails without contacting the provider', () => {
  const f = fixture(); const root = f.root();
  root.elements.delete('[data-map-canvas]');
  const c = initializeMapPanel(root, KEY, f.environment);
  assert.equal(c.state, 'failed'); assert.equal(f.scripts.length, 0);
});
