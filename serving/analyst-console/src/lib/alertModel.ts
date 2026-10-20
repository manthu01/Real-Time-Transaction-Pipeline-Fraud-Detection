import type { Alert, ShapFeature, TopFeature, ZScoreFeature } from './types';

/** Alerts ingested before the LightGBM champion have no SHAP fields:
 * their `score` is an Isolation Forest anomaly score and their
 * topFeatures are z-scores. Every view branches on this, rather than
 * guessing from the feature shape alone. */
export function isLegacyAlert(alert: Alert): boolean {
  return alert.baseValue == null;
}

export function isShapFeature(f: TopFeature): f is ShapFeature {
  return typeof (f as ShapFeature).shap === 'number';
}

export function shapFeatures(alert: Alert): ShapFeature[] {
  return alert.topFeatures.filter(isShapFeature);
}

export function zScoreFeatures(features: TopFeature[] | null): ZScoreFeature[] {
  return (features ?? []).filter((f): f is ZScoreFeature => typeof (f as ZScoreFeature).zScore === 'number');
}

/** Where the SHAP waterfall ends: the model's log-odds output. rawScore
 * is sent with every SHAP alert; logit(score) is only a fallback for a
 * row written without it, clamped because a stored p of exactly 1.0 (or
 * 0.0) has no finite log-odds. */
export function modelOutputLogOdds(alert: Alert): number {
  if (alert.rawScore != null) return alert.rawScore;
  const p = Math.min(Math.max(alert.score, 1e-9), 1 - 1e-9);
  return Math.log(p / (1 - p));
}

/** LightGBM probabilities near the capacity threshold are ~3e-5, which
 * toFixed(3) would flatten to "0.000" -- switch to exponent form there. */
export function formatProbability(p: number): string {
  if (p === 0 || p >= 0.001) return p.toFixed(3);
  return p.toExponential(1);
}

/** The feed/detail "score" cell: a probability for current alerts, the
 * raw anomaly score (not a probability) for legacy ones. */
export function formatAlertScore(alert: Alert): string {
  return isLegacyAlert(alert) ? alert.score.toFixed(3) : formatProbability(alert.score);
}

export function formatFeatureValue(value: number | null): string {
  if (value == null) return 'missing';
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}
