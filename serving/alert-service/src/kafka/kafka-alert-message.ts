import { TopFeature, ZScoreFeature } from '../alerts/alert.entity';

/** Shape of a message on fraud.alerts, produced by
 * streaming/pulseguard_streaming/alert_scoring.py. Kept as a plain
 * interface (not a class) since it's a wire contract, not a domain
 * object -- the domain object is the Alert entity it gets mapped into.
 *
 * baseValue/rawScore/isoScore/unusualFeatures arrived with the LightGBM
 * champion and are optional: fraud.alerts keeps 30 days of messages, so a
 * consumer replaying the topic still meets the older z-score-only shape. */
export interface KafkaAlertMessage {
  alertId: string;
  txnId: string;
  customerId: string;
  timestamp: string;
  score: number;
  severity: string;
  triggeredRules: string[];
  topFeatures: TopFeature[];
  baseValue?: number | null;
  rawScore?: number | null;
  isoScore?: number | null;
  unusualFeatures?: ZScoreFeature[] | null;
  trailingMean: number | null;
  trailingMax: number | null;
  amount: number | null;
  channel: string | null;
  merchantId: string | null;
  merchantCategory: string | null;
  city: string | null;
  lat: number | null;
  lon: number | null;
}

const isOptionalNumber = (v: unknown) => v === undefined || v === null || typeof v === 'number';
const isOptionalArray = (v: unknown) => v === undefined || v === null || Array.isArray(v);
const isNamedObject = (v: unknown) => typeof v === 'object' && v !== null && typeof (v as { name?: unknown }).name === 'string';

export function isValidKafkaAlertMessage(value: unknown): value is KafkaAlertMessage {
  if (typeof value !== 'object' || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.alertId === 'string' &&
    typeof v.txnId === 'string' &&
    typeof v.customerId === 'string' &&
    typeof v.timestamp === 'string' &&
    typeof v.score === 'number' &&
    typeof v.severity === 'string' &&
    Array.isArray(v.triggeredRules) &&
    Array.isArray(v.topFeatures) &&
    v.topFeatures.every(isNamedObject) &&
    isOptionalNumber(v.baseValue) &&
    isOptionalNumber(v.rawScore) &&
    isOptionalNumber(v.isoScore) &&
    isOptionalArray(v.unusualFeatures)
  );
}
