/** An OpenStreetMap iframe embed centered on the transaction's
 * coordinates -- no API key, no mapping-library dependency, "functional
 * over beautiful" for a single-marker geo view. A drill-down needing more
 * than "where did this happen" would warrant react-leaflet; this doesn't. */
export function GeoEmbed({ lat, lon, label }: { lat: number; lon: number; label: string }) {
  const delta = 0.15;
  const bbox = `${lon - delta}%2C${lat - delta}%2C${lon + delta}%2C${lat + delta}`;
  const src = `https://www.openstreetmap.org/export/embed.html?bbox=${bbox}&layer=mapnik&marker=${lat}%2C${lon}`;

  return (
    <div>
      <iframe
        title={`map: ${label}`}
        src={src}
        style={{ width: '100%', height: 220, border: '1px solid var(--panel-border)', borderRadius: 6 }}
        loading="lazy"
      />
      <p className="muted" style={{ marginTop: 6 }}>
        {label} ({lat.toFixed(4)}, {lon.toFixed(4)})
      </p>
    </div>
  );
}
