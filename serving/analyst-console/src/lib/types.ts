/** Mirrors serving/alert-service/src/alerts/alert.entity.ts -- kept as a
 * separate, hand-written type here rather than shared via a package,
 * since this is the only consumer and a shared-types package would be
 * more machinery than the project's size justifies. */

export type DispositionStatus = 'PENDING' | 'CONFIRMED_FRAUD' | 'FALSE_POSITIVE' | 'NEEDS_INVESTIGATION';
export type Severity = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';

/** TreeSHAP contribution from the LightGBM champion, in log-odds;
 * direction "up" = pushed toward fraud. value null = feature was missing. */
export interface ShapFeature {
  name: string;
  value: number | null;
  shap: number;
  direction: 'up' | 'down';
}

/** Standard deviations from the non-fraud population. Pre-LightGBM alerts
 * carry these as topFeatures; current ones as unusualFeatures. */
export interface ZScoreFeature {
  name: string;
  value: number;
  zScore: number;
}

export type TopFeature = ShapFeature | ZScoreFeature;

export interface Alert {
  alertId: string;
  txnId: string;
  customerId: string;
  timestamp: string;
  score: number;
  severity: Severity;
  triggeredRules: string[];
  topFeatures: TopFeature[];
  // null on alerts ingested before the LightGBM champion -- see isLegacyAlert
  baseValue: number | null;
  rawScore: number | null;
  isoScore: number | null;
  unusualFeatures: ZScoreFeature[] | null;
  trailingMean: number | null;
  trailingMax: number | null;
  amount: number | null;
  channel: string | null;
  merchantId: string | null;
  merchantCategory: string | null;
  city: string | null;
  lat: number | null;
  lon: number | null;
  dispositionStatus: DispositionStatus;
  dispositionNote: string | null;
  dispositionedAt: string | null;
  ingestedAt: string;
}

export interface AlertListResponse {
  items: Alert[];
  total: number;
}

export interface AlertDetailResponse {
  alert: Alert;
  recentForCustomer: Alert[];
}

export interface RulePrecision {
  rule: string;
  confirmedFraud: number;
  falsePositive: number;
  precision: number | null;
}

export interface AlertsPerDay {
  day: string;
  count: number;
}

export interface FalsePositiveRatePoint {
  day: string;
  falsePositiveRate: number | null;
  dispositionedCount: number;
}

export interface MetricsSummary {
  alertsPerDay: AlertsPerDay[];
  precisionByRule: RulePrecision[];
  meanTimeToDispositionSeconds: number | null;
  falsePositiveRateTrend: FalsePositiveRatePoint[];
}
