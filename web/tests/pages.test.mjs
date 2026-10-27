import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

// Reads the built site; run after `astro build` (see `npm run test:pages`).
const page = (path) => readFileSync(new URL(`../dist/${path}`, import.meta.url), 'utf8').replaceAll('&#39;', "'");

test('map page is titled Linzi\'s Data Lab with a visually hidden h1 and no leftover copy', () => {
  const html = page('index.html');
  assert.match(html, /<title>Linzi's Data Lab<\/title>/);
  assert.match(html, /<h1[^>]*class="[^"]*sr-only[^"]*"[^>]*>Linzi's Data Lab<\/h1>/);
  for (const leftover of ['Transit Data Project', 'Exploring transit data', 'Map preview', 'About this project', 'DE Portfolio']) {
    assert.equal(html.includes(leftover), false, `found "${leftover}"`);
  }
});

test('map page workspace holds the map panel then the chat panel placeholder', () => {
  const html = page('index.html');
  const workspace = html.match(/<div[^>]*class="[^"]*\bworkspace\b[^"]*"[^>]*>[\s\S]*$/)?.[0];
  assert.ok(workspace, 'workspace missing');
  const map = workspace.indexOf('data-map-canvas');
  const chat = workspace.indexOf('AI chat — coming soon');
  assert.ok(map !== -1 && chat !== -1 && map < chat, 'expected map panel before chat panel');
  assert.match(workspace, /The map couldn't load right now\./);
});

for (const path of ['index.html', 'blog/index.html']) {
  test(`${path} header shows the logo and brand and offers no theme switch`, () => {
    const html = page(path);
    assert.match(html, /<a[^>]*class="[^"]*site-brand[^"]*"[^>]*>\s*<svg[^>]*class="[^"]*site-logo[\s\S]*?Linzi's Data Lab[\s\S]*?<\/a>/);
    assert.equal(html.includes('starlight-theme-select'), false);
  });
}

test('blog page is titled Blog', () => {
  const html = page('blog/index.html');
  assert.match(html, /<title>Blog \| Linzi's Data Lab<\/title>/);
  assert.equal(html.includes('DE Learning Notes'), false);
});
