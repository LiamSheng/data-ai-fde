interface NavigationEntry {
  id: 'map' | 'blog';
  label: string;
  href: string;
}

export const navigation: readonly NavigationEntry[] = [
  { id: 'map', label: 'Map', href: '/' },
  { id: 'blog', label: 'Blog', href: '/blog/' },
];

/** Off-site profiles, shown as icons beside the pages; the label is the link's accessible name. */
export const profiles: readonly { label: string; href: string }[] = [
  { label: 'GitHub', href: 'https://github.com/LiamSheng' },
];
