/** Mirrors serving/alert-service/src/alerts/alert.entity.ts -- kept as a
 * separate, hand-written type here rather than shared via a package,
 * since this is the only consumer and a shared-types package would be
 * more machinery than the project's size justifies. */

export type DispositionStatus = 'PENDING' | 'CONFIRMED_FRAUD' | 'FALSE_POSITIVE' | 'NEEDS_INVESTIGATION';
export type Severity = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';

export interface TopFeature {
  name: string;
  value: number;
  zScore: number;
}

export interface Alert {
  alertId: string;
  txnId: string;
  customerId: string;
  timestamp: string;
  score: number;
  severity: Severity;
  triggeredRules: string[];
  topFeatures: TopFeature[];
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
