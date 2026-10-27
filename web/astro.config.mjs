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
