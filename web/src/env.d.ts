/// <reference types="astro/client" />
/// <reference types="@astrojs/starlight" />

interface ImportMetaEnv {
  readonly PUBLIC_GOOGLE_MAPS_API_KEY?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
