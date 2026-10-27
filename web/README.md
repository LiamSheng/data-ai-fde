# web

Linzi's Data Lab: an Astro static site. The Map page (`/`) holds the Workspace, a
Map panel (Google Maps, Greater Vancouver) beside a Chat panel that is a placeholder
for the agent service. The Blog (`/blog/`) is built with Starlight.

## Requirements

Node 24 (see `.nvmrc`). `npm run test:map` executes TypeScript directly and needs Node 24.

## Commands

```sh
cd web
npm ci
npm run test:map
npm run check
npm run build
npm run dev -- --host 127.0.0.1 --port 4321
npm run preview -- --host 127.0.0.1 --port 4322
```

## Configuration

Copy `.env.example` to `.env` and set `PUBLIC_GOOGLE_MAPS_API_KEY`. Without it the
map shows "Map setup pending" and does not load the Google Maps SDK. `PUBLIC_`
values are compiled into the browser bundle at build time; restrict the key at Google.
