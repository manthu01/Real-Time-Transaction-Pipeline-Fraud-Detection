import { Column, CreateDateColumn, Entity, Index, PrimaryColumn } from 'typeorm';

export enum DispositionStatus {
  PENDING = 'PENDING',
  CONFIRMED_FRAUD = 'CONFIRMED_FRAUD',
  FALSE_POSITIVE = 'FALSE_POSITIVE',
  NEEDS_INVESTIGATION = 'NEEDS_INVESTIGATION',
}

export interface TopFeature {
  name: string;
  value: number;
  zScore: number;
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

  @Column({ type: 'double precision' })
  score: number;

  @Index()
  @Column()
  severity: string;

  @Column({ type: 'jsonb', default: () => "'[]'" })
  triggeredRules: string[];

  @Column({ type: 'jsonb', default: () => "'[]'" })
  topFeatures: TopFeature[];

  @Column({ type: 'double precision', nullable: true })
  trailingMean: number | null;

  @Column({ type: 'double precision', nullable: true })
  trailingMax: number | null;

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
