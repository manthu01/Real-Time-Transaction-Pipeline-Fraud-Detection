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

  it('accepts a LightGBM champion message with SHAP features, base value, raw score and shadow score', () => {
    const shapMessage = {
      ...VALID,
      topFeatures: [
        { name: 'txnCount1Min', value: 6, shap: 28.34, direction: 'up' },
        { name: 'secondsSincePrevTxn', value: null, shap: 0.24, direction: 'up' },
      ],
      baseValue: -10.4243,
      rawScore: 18.2,
      isoScore: 0.4546,
      unusualFeatures: [{ name: 'txnCount1Min', value: 6, zScore: 30.85 }],
    };
    expect(isValidKafkaAlertMessage(shapMessage)).toBe(true);
  });

  it('accepts a pre-LightGBM message: z-score topFeatures and no SHAP fields (still on the topic for 30 days)', () => {
    const legacy = { ...VALID, topFeatures: [{ name: 'impliedSpeedKmh', value: 3000, zScore: 12.4 }] };
    expect(isValidKafkaAlertMessage(legacy)).toBe(true);
  });

  it.each(['baseValue', 'rawScore', 'isoScore'])('rejects a message where %s is present but not a number', (field) => {
    expect(isValidKafkaAlertMessage({ ...VALID, [field]: '0.5' })).toBe(false);
  });

  it('rejects a message whose topFeatures entries are not named objects', () => {
    expect(isValidKafkaAlertMessage({ ...VALID, topFeatures: [1.5] })).toBe(false);
  });

  it('rejects a message where unusualFeatures is present but not an array', () => {
    expect(isValidKafkaAlertMessage({ ...VALID, unusualFeatures: {} })).toBe(false);
  });
});
