/**
 * The base map in grey, the roads in cobalt. A style only reaches the SDK's own tiles,
 * so anything drawn on top later (markers, route lines) keeps its own colour.
 * The SDK takes hex, not CSS variables; each value is the nearest hex to a Cobalt token in src/styles/tokens.css.
 * JSON styles only apply to a map without a mapId; a map that needs one (advanced markers) moves this to a cloud style.
 */
const PAPER_2 = '#eff2f6'; // --color-paper-2
const PAPER_3 = '#e3e8ef'; // --color-paper-3
const RULE_2 = '#c9cfd8'; // --color-rule-2
const MUTED = '#626b78'; // --color-muted
const INK_2 = '#3d4553'; // --color-ink-2
const ACCENT = '#2f62d9'; // --color-accent
const ACCENT_2 = '#86aef2'; // --color-accent-2
const ACCENT_WASH = '#e5edfb'; // --color-accent-wash

export const MAP_STYLE: google.maps.MapTypeStyle[] = [
  { elementType: 'geometry', stylers: [{ color: PAPER_2 }] },
  { elementType: 'labels.icon', stylers: [{ saturation: -100 }, { lightness: 20 }] },
  { elementType: 'labels.text.fill', stylers: [{ color: MUTED }] },
  { elementType: 'labels.text.stroke', stylers: [{ color: PAPER_2 }] },
  { featureType: 'administrative', elementType: 'geometry.stroke', stylers: [{ color: RULE_2 }] },
  { featureType: 'administrative.locality', elementType: 'labels.text.fill', stylers: [{ color: INK_2 }] },
  { featureType: 'landscape.natural', elementType: 'geometry', stylers: [{ color: PAPER_3 }] },
  { featureType: 'poi', stylers: [{ visibility: 'off' }] },
  { featureType: 'poi.park', elementType: 'geometry', stylers: [{ visibility: 'on' }, { color: PAPER_3 }] },
  { featureType: 'water', elementType: 'geometry', stylers: [{ color: RULE_2 }] },
  { featureType: 'water', elementType: 'labels.text.fill', stylers: [{ color: PAPER_2 }] },
  { featureType: 'road', elementType: 'geometry', stylers: [{ color: ACCENT_WASH }] },
  { featureType: 'road.arterial', elementType: 'geometry', stylers: [{ color: ACCENT_2 }] },
  { featureType: 'road.highway', elementType: 'geometry', stylers: [{ color: ACCENT }] },
  { featureType: 'road', elementType: 'labels.icon', stylers: [{ visibility: 'off' }] },
  { featureType: 'transit.line', elementType: 'geometry', stylers: [{ color: ACCENT_2 }] },
];
