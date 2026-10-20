import { formatFeatureValue, formatProbability } from '@/lib/alertModel';
import type { ShapFeature } from '@/lib/types';

interface Step {
  key: string;
  label: string;
  detail: string;
  from: number;
  to: number;
  kind: 'up' | 'down' | 'other';
}

const sigmoid = (x: number) => 1 / (1 + Math.exp(-x));
const signed = (x: number) => `${x >= 0 ? '+' : '−'}${Math.abs(x).toFixed(2)}`;

/**
 * SHAP waterfall, hand-rolled in the same style as ZScoreBars: starts at
 * the model's base value (its average output, in log-odds), then each of
 * the top features pushes it -- red toward fraud, blue away -- and ends
 * at the model's actual output for this transaction. Every bar sits on
 * one shared log-odds axis, so lengths are comparable across rows.
 *
 * Only the top 5 features are sent per alert, so an "all other features"
 * step carries the remainder (rawScore - baseValue - sum of the top 5);
 * with it, the chart adds up exactly to the model's output -- the SHAP
 * additivity property, made visible.
 */
export function ShapWaterfall({ features, baseValue, rawScore }: { features: ShapFeature[]; baseValue: number; rawScore: number }) {
  const steps: Step[] = [];
  let cursor = baseValue;
  for (const f of features) {
    steps.push({ key: f.name, label: f.name, detail: formatFeatureValue(f.value), from: cursor, to: cursor + f.shap, kind: f.shap >= 0 ? 'up' : 'down' });
    cursor += f.shap;
  }
  const remainder = rawScore - cursor;
  if (Math.abs(remainder) >= 0.005) {
    steps.push({ key: '__other', label: 'all other features', detail: '', from: cursor, to: rawScore, kind: 'other' });
  }

  const points = [baseValue, rawScore, ...steps.flatMap((s) => [s.from, s.to])];
  const lo = Math.min(...points);
  const hi = Math.max(...points);
  const pad = Math.max((hi - lo) * 0.05, 0.25);
  const min = lo - pad;
  const span = hi + pad - min;
  const pct = (x: number) => ((x - min) / span) * 100;
  // log-odds 0 is p = 0.5: worth marking whenever the chart crosses it
  const showMidline = min < 0 && min + span > 0;

  const marker = (x: number) => (
    <div className="waterfall-track">
      {showMidline && <div className="waterfall-midline" style={{ left: `${pct(0)}%` }} />}
      <div className="waterfall-marker" style={{ left: `${pct(x)}%` }} />
    </div>
  );

  return (
    <div className="waterfall">
      <div className="bar-row waterfall-row">
        <span title="the model's average output over the training data">base value</span>
        {marker(baseValue)}
        <span className="muted">{baseValue.toFixed(2)}</span>
      </div>

      {steps.map((s) => (
        <div className="bar-row waterfall-row" key={s.key}>
          <span title={s.label}>
            {s.label}
            {s.detail && <span className="muted"> = {s.detail}</span>}
          </span>
          <div className="waterfall-track">
            {showMidline && <div className="waterfall-midline" style={{ left: `${pct(0)}%` }} />}
            <div
              className={`waterfall-bar ${s.kind}`}
              style={{ left: `${pct(Math.min(s.from, s.to))}%`, width: `${Math.max((Math.abs(s.to - s.from) / span) * 100, 0.6)}%` }}
            />
          </div>
          <span className={`waterfall-delta ${s.kind}`}>{signed(s.to - s.from)}</span>
        </div>
      ))}

      <div className="bar-row waterfall-row waterfall-total">
        <span>model output</span>
        {marker(rawScore)}
        <span className="muted">{rawScore.toFixed(2)}</span>
      </div>

      <p className="muted waterfall-caption">
        log-odds axis from {min.toFixed(1)} to {(min + span).toFixed(1)}
        {showMidline && <> · dashed line = p 0.5</>} · output {rawScore.toFixed(2)} → p = {formatProbability(sigmoid(rawScore))}
        <span className="waterfall-legend">
          <span className="waterfall-swatch up" /> toward fraud <span className="waterfall-swatch down" /> away from fraud
        </span>
      </p>
    </div>
  );
}
