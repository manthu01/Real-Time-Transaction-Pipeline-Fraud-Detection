import { formatFeatureValue } from '@/lib/alertModel';
import type { ZScoreFeature } from '@/lib/types';

/** A hand-rolled horizontal bar per feature, scaled by |zScore| against
 * the largest magnitude in the set -- "functional over beautiful" per
 * the build plan; a full charting library is more than 3 bars need.
 * Answers "what's statistically odd about this transaction", not "why
 * did the model flag it" (that's ShapWaterfall): it's the main view for
 * pre-LightGBM alerts and the "Statistically unusual" view for current ones. */
export function ZScoreBars({ features }: { features: ZScoreFeature[] }) {
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
              {/* neutral, not red/blue: those mean toward/away from fraud in
                  ShapWaterfall right above this, and a z-score has no direction
                  of "fraud-ness" -- only distance from normal */}
              <div className="bar-fill zscore" style={{ width: `${pct}%` }} />
            </div>
            <span className="muted">
              {formatFeatureValue(f.value)} (z={f.zScore >= 0 ? '+' : '−'}
              {Math.abs(f.zScore).toFixed(2)})
            </span>
          </div>
        );
      })}
    </div>
  );
}
