interface NavigationEntry {
  id: 'map' | 'blog';
  label: string;
  href: string;
}

export const navigation: readonly NavigationEntry[] = [
  { id: 'map', label: 'Map', href: '/' },
  { id: 'blog', label: 'Blog', href: '/blog/' },
];
