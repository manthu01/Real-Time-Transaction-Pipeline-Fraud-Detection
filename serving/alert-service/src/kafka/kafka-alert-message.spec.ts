import { isValidKafkaAlertMessage } from './kafka-alert-message';

const VALID = {
  alertId: 'a1',
  txnId: 't1',
  customerId: 'C1',
  timestamp: '2026-03-10T10:00:00Z',
  score: 0.5,
  severity: 'HIGH',
  triggeredRules: [],
  topFeatures: [],
};

describe('isValidKafkaAlertMessage', () => {
  it('accepts a well-formed message', () => {
    expect(isValidKafkaAlertMessage(VALID)).toBe(true);
  });

  it('rejects null and non-objects', () => {
    expect(isValidKafkaAlertMessage(null)).toBe(false);
    expect(isValidKafkaAlertMessage('a string')).toBe(false);
    expect(isValidKafkaAlertMessage(42)).toBe(false);
  });

  it.each(['alertId', 'txnId', 'customerId', 'timestamp', 'severity'])('rejects a message missing %s', (field) => {
    const { [field]: _omitted, ...rest } = VALID as Record<string, unknown>;
    expect(isValidKafkaAlertMessage(rest)).toBe(false);
  });

  it('rejects a message where score is not a number', () => {
    expect(isValidKafkaAlertMessage({ ...VALID, score: '0.5' })).toBe(false);
  });

  it('rejects a message where triggeredRules is not an array', () => {
    expect(isValidKafkaAlertMessage({ ...VALID, triggeredRules: 'IMPOSSIBLE_TRAVEL' })).toBe(false);
  });
});
