import { MAP_STYLE } from './map-style.ts';

/** A single document owns its SDK request; each map panel owns its map and state. */
type MapState = 'waiting' | 'loading' | 'loaded' | 'failed';
type BrowserWindow = Window & typeof globalThis;
type Environment = { window: BrowserWindow; document: Document };

export interface MapPanelController {
  readonly state: MapState;
  readonly map: google.maps.Map | undefined;
  destroy(): void;
}

interface SdkSession {
  ready: Promise<typeof google.maps>;
  authFailed: boolean;
  authListeners: Set<() => void>;
}

const sdkSessions = new WeakMap<Document, SdkSession>();
const panels = new WeakMap<HTMLElement, MapPanelController>();
const TIMEOUT_MS = 15_000;
let callbackSequence = 0;

function getSdkSession(key: string, { window: win, document: doc }: Environment): SdkSession {
  const existing = sdkSessions.get(doc);
  if (existing) return existing;

  const globals = win as unknown as Record<string, unknown>;
  const previousAuthFailure = globals.gm_authFailure;
  let rejectLoading = (_reason: string) => {};
  const session: SdkSession = {
    ready: Promise.resolve(undefined as unknown as typeof google.maps),
    authFailed: false,
    authListeners: new Set(),
  };
  sdkSessions.set(doc, session);

  const authFailure = () => {
    session.authFailed = true;
    rejectLoading('provider-authentication');
    for (const listener of session.authListeners) listener();
    if (typeof previousAuthFailure === 'function') {
      try { previousAuthFailure.call(win); } catch { /* Preserve our failure state. */ }
    }
  };
  globals.gm_authFailure = authFailure;

  let callbackName = `__portfolioMapsReady${++callbackSequence}`;
  while (callbackName in globals) callbackName = `__portfolioMapsReady${++callbackSequence}`;
  let script: HTMLScriptElement | undefined;
  let timer: number | undefined;
  let settled = false;
  let cancelLoading = () => {};
  session.ready = new Promise((resolve, reject) => {
    const settle = (reason?: string) => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) win.clearTimeout(timer);
      script?.removeEventListener('error', onError);
      // A late SDK callback after a timeout must be harmless until page unload.
      globals[callbackName] = () => {};
      if (reason) reject(new Error(reason));
      else resolve(win.google.maps);
    };
    const onError = () => settle('sdk-network');
    rejectLoading = (reason) => settle(reason);
    cancelLoading = () => settle('document-unloaded');
    globals[callbackName] = () => {
      if (session.authFailed) settle('provider-authentication');
      else if (typeof win.google?.maps?.Map !== 'function') settle('sdk-unavailable');
      else settle();
    };

    if (typeof win.google?.maps?.Map === 'function') {
      settle();
      return;
    }

    script = doc.createElement('script');
    script.async = true;
    script.dataset.portfolioMapsSdk = '';
    const parameters = new URLSearchParams({
      key, loading: 'async', callback: callbackName,
      v: 'quarterly', language: 'en', region: 'CA',
    });
    script.src = `https://maps.googleapis.com/maps/api/js?${parameters}`;
    script.addEventListener('error', onError, { once: true });
    timer = win.setTimeout(() => settle('sdk-timeout'), TIMEOUT_MS);
    doc.head.append(script);
  });

  const onPageHide = (event?: PageTransitionEvent) => {
    // A cached document can return through Back; retain its live map/callbacks.
    if (event?.persisted) return;
    cancelLoading();
    session.authListeners.clear();
    if (globals.gm_authFailure === authFailure) {
      if (previousAuthFailure === undefined) delete globals.gm_authFailure;
      else globals.gm_authFailure = previousAuthFailure;
    }
    delete globals[callbackName];
    win.removeEventListener('pagehide', onPageHide);
  };
  win.addEventListener('pagehide', onPageHide);
  return session;
}

/** Importing this module is safe on the server; browser globals are read here only. */
export function initializeMapPanel(
  root: HTMLElement,
  key: string | undefined,
  environment?: Environment,
): MapPanelController {
  const existing = panels.get(root);
  if (existing) return existing;
  const { window: win, document: doc } = environment ?? { window, document };
  const canvas = root.querySelector<HTMLElement>('[data-map-canvas]');
  let state: MapState = 'waiting';
  let map: google.maps.Map | undefined;
  let destroyed = false;
  let tileTimer: number | undefined;
  let tileListener: google.maps.MapsEventListener | undefined;
  let session: SdkSession | undefined;

  const connected = () => !destroyed && root.isConnected;
  const setState = (next: MapState) => {
    if (!connected()) return;
    state = next;
    root.dataset.mapState = next;
  };
  const clearTiles = () => {
    if (tileTimer !== undefined) win.clearTimeout(tileTimer);
    tileTimer = undefined;
    tileListener?.remove();
    tileListener = undefined;
  };
  const fail = () => {
    if (!connected()) return;
    clearTiles();
    setState('failed');
  };
  const controller: MapPanelController = {
    get state() { return state; },
    get map() { return map; },
    destroy() {
      if (destroyed) return;
      destroyed = true;
      clearTiles();
      session?.authListeners.delete(fail);
      win.removeEventListener('pagehide', onPageHide);
      map = undefined;
    },
  };
  const onPageHide = (event?: PageTransitionEvent) => {
    if (!event?.persisted) controller.destroy();
  };
  panels.set(root, controller);
  setState('waiting');
  const normalizedKey = key?.trim();
  if (!normalizedKey || !connected()) return controller;
  if (!canvas) {
    fail();
    return controller;
  }

  setState('loading');
  session = getSdkSession(normalizedKey, { window: win, document: doc });
  session.authListeners.add(fail);
  win.addEventListener('pagehide', onPageHide);
  if (session.authFailed) fail();

  void session.ready.then((maps) => {
    if (!connected() || state === 'failed') return;
    if (typeof maps.version === 'string') root.dataset.mapSdkVersion = maps.version;
    try {
      map = new maps.Map(canvas, {
        center: { lat: 49.1044, lng: -122.6604 }, zoom: 14, mapTypeId: 'roadmap', styles: MAP_STYLE,
        // No buttons on the map; drag pans and the wheel zooms (up to zoom in) without a modifier or an overlay.
        disableDefaultUI: true, gestureHandling: 'greedy',
      });
      if (!connected() || controller.state === 'failed') return;
      tileTimer = win.setTimeout(fail, TIMEOUT_MS);
      tileListener = map.addListener('tilesloaded', () => {
        if (!connected() || state !== 'loading') return;
        clearTiles();
        setState('loaded');
      });
    } catch {
      fail();
    }
  }).catch(fail);
  return controller;
}
