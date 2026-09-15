import type { TopFeature } from '@/lib/types';

/** A hand-rolled horizontal bar per feature, scaled by |zScore| against
 * the largest magnitude in the set -- "functional over beautiful" per
 * the build plan; a full charting library is more than 3 bars need. */
export function TopFeaturesBars({ features }: { features: TopFeature[] }) {
  if (features.length === 0) return <p className="muted">No standout features (model-only score below the alerting rules&apos; own signals).</p>;

  const maxAbs = Math.max(...features.map((f) => Math.abs(f.zScore)), 1);

  return (
    <div>
      {features.map((f) => {
        const pct = Math.min((Math.abs(f.zScore) / maxAbs) * 100, 100);
        return (
          <div className="bar-row" key={f.name}>
            <span title={f.name}>{f.name}</span>
            <div className="bar-track">
              <div className={`bar-fill ${f.zScore < 0 ? 'negative' : ''}`} style={{ width: `${pct}%` }} />
            </div>
            <span className="muted">
              {f.value} (z={f.zScore.toFixed(2)})
            </span>
          </div>
        );
      })}
    </div>
  );
}
