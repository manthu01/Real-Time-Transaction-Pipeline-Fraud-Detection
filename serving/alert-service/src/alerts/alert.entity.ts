import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

export enum DispositionStatus {
  PENDING = 'PENDING',
  CONFIRMED_FRAUD = 'CONFIRMED_FRAUD',
  FALSE_POSITIVE = 'FALSE_POSITIVE',
  NEEDS_INVESTIGATION = 'NEEDS_INVESTIGATION',
}

/** One feature's TreeSHAP contribution from the LightGBM champion, in
 * log-odds. direction "up" = pushed toward fraud. value is null when the
 * feature was missing (e.g. secondsSincePrevTxn on a first transaction). */
export interface ShapFeature {
  name: string;
  value: number | null;
  shap: number;
  direction: 'up' | 'down';
}

/** How far a feature sits from the non-fraud population, in standard
 * deviations. Alerts from before the LightGBM champion carried these as
 * their topFeatures; newer alerts carry them as unusualFeatures. */
export interface ZScoreFeature {
  name: string;
  value: number;
  zScore: number;
}

/** topFeatures is SHAP on current alerts and z-scores on rows ingested
 * before the switch -- both shapes stay readable rather than
 * back-filling old rows with explanations their model never produced. */
export type TopFeature = ShapFeature | ZScoreFeature;

export function isShapFeature(feature: TopFeature): feature is ShapFeature {
  return typeof (feature as ShapFeature).shap === 'number';
}

/**
 * Mirrors the alert record published to the fraud.alerts Kafka topic by
 * the Spark job's alert_scoring stage (see streaming/pulseguard_streaming/
 * alert_scoring.py), plus the disposition fields this service owns.
 * alertId is the primary key rather than an auto-generated one so
 * re-consuming the same Kafka message twice (e.g. after a consumer
 * restart before offsets commit) upserts instead of duplicating.
 */
@Entity('alerts')
export class Alert {
  @PrimaryColumn('uuid')
  alertId: string;

  @Index()
  @Column()
  txnId: string;

  @Index()
  @Column()
  customerId: string;

  @Index()
  @Column({ type: 'timestamptz' })
  timestamp: Date;

  /** LightGBM fraud probability. On rows ingested before the LightGBM
   * champion (baseValue IS NULL), this is the Isolation Forest anomaly
   * score instead. */
  @Column({ type: 'double precision' })
  score: number;

  @Index()
  @Column()
  severity: string;

  @Column({ type: 'jsonb', default: () => "'[]'" })
  triggeredRules: string[];

  @Column({ type: 'jsonb', default: () => "'[]'" })
  topFeatures: TopFeature[];

  // The SHAP / champion-challenger columns below are nullable because rows
  // ingested before the LightGBM champion don't have them; null means
  // "not recorded", which the console renders as the legacy view.

  /** SHAP base value: the model's average output, in log-odds. */
  @Column({ type: 'double precision', nullable: true })
  baseValue: number | null;

  /** The model's log-odds output (baseValue + every feature's SHAP). */
  @Column({ type: 'double precision', nullable: true })
  rawScore: number | null;

  /** Isolation Forest score, scored in shadow (never triggers an alert). */
  @Column({ type: 'double precision', nullable: true })
  isoScore: number | null;

  @Column({ type: 'jsonb', nullable: true })
  unusualFeatures: ZScoreFeature[] | null;

  @Column({ type: 'double precision', nullable: true })
  trailingMean: number | null;

  @Column({ type: 'double precision', nullable: true })
  trailingMax: number | null;

  @Column({ type: 'double precision', nullable: true })
  amount: number | null;

  // TypeORM infers a column's SQL type from TypeScript's emitted reflection
  // metadata, which collapses any `string | null` union down to `Object`
  // (not `String`) -- an explicit `type: 'varchar'` is required for every
  // nullable string column here, not just the pattern-setting one
  // (dispositionNote, above) that happened to already have one.
  @Column({ type: 'varchar', nullable: true })
  channel: string | null;

  @Column({ type: 'varchar', nullable: true })
  merchantId: string | null;

  @Column({ type: 'varchar', nullable: true })
  merchantCategory: string | null;

  @Column({ type: 'varchar', nullable: true })
  city: string | null;

  @Column({ type: 'double precision', nullable: true })
  lat: number | null;

  @Column({ type: 'double precision', nullable: true })
  lon: number | null;

  @Index()
  @Column({ type: 'enum', enum: DispositionStatus, default: DispositionStatus.PENDING })
  dispositionStatus: DispositionStatus;

  @Column({ type: 'text', nullable: true })
  dispositionNote: string | null;

  @Column({ type: 'timestamptz', nullable: true })
  dispositionedAt: Date | null;

  @CreateDateColumn({ type: 'timestamptz' })
  ingestedAt: Date;
}
