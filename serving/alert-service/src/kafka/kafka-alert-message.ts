import { TopFeature } from '../alerts/alert.entity';

/** Shape of a message on fraud.alerts, produced by
 * streaming/pulseguard_streaming/alert_scoring.py. Kept as a plain
 * interface (not a class) since it's a wire contract, not a domain
 * object -- the domain object is the Alert entity it gets mapped into. */
export interface KafkaAlertMessage {
  alertId: string;
  txnId: string;
  customerId: string;
  timestamp: string;
  score: number;
  severity: string;
  triggeredRules: string[];
  topFeatures: TopFeature[];
  trailingMean: number | null;
  trailingMax: number | null;
}

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
    Array.isArray(v.topFeatures)
  );
}
