import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightThemeBlack from 'starlight-theme-black';
import { SITE_NAME } from './src/config/site.ts';

export default defineConfig({
  output: 'static',
  build: { format: 'directory' },
  trailingSlash: 'always',
  server: { host: '127.0.0.1', port: 4321 },
  integrations: [
    starlight({
      title: SITE_NAME,
      locales: { root: { label: 'English', lang: 'en' } },
      pagefind: false,
      // The Cobalt faces (src/styles/tokens.css): Familjen Grotesk heads, IBM Plex Sans body, JetBrains Mono for the field (700 for its code).
      head: [
        { tag: 'link', attrs: { rel: 'preconnect', href: 'https://fonts.googleapis.com' } },
        { tag: 'link', attrs: { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: true } },
        {
          tag: 'link',
          attrs: {
            rel: 'stylesheet',
            href: 'https://fonts.googleapis.com/css2?family=Familjen+Grotesk:wght@500;600&family=IBM+Plex+Sans:wght@400;500&family=JetBrains+Mono:wght@400;500;700&display=swap',
          },
        },
      ],
      sidebar: [{ label: 'Blog', link: '/blog/' }],
      components: {
        Header: './src/components/SiteHeader.astro',
        PageTitle: './src/components/PageTitle.astro',
        ThemeProvider: './src/components/ThemeProvider.astro',
      },
      customCss: ['./src/styles/global.css'],
      plugins: [starlightThemeBlack({ docs: { showMarkdownActions: false } })],
    }),
  ],
});
