import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { KafkaAlertMessage } from '../kafka/kafka-alert-message';
import { Alert, DispositionStatus } from './alert.entity';
import { CreateDispositionDto } from './dto/create-disposition.dto';
import { ListAlertsDto } from './dto/list-alerts.dto';

export interface DispositionResult {
  alert: Alert;
  dispositionEvent: {
    alertId: string;
    customerId: string;
    status: DispositionStatus;
    note: string | null;
    dispositionedAt: string;
  };
}

@Injectable()
export class AlertsService {
  private readonly logger = new Logger(AlertsService.name);

  constructor(@InjectRepository(Alert) private readonly repo: Repository<Alert>) {}

  /** Upserts an alert from a Kafka message. Keyed by alertId so
   * re-consuming the same message (e.g. after a consumer restart before
   * offsets committed) is idempotent rather than creating a duplicate row
   * -- the exactly-once-processing concern applies here just as much as
   * in the Spark job. */
  async ingestFromKafka(message: KafkaAlertMessage): Promise<Alert> {
    const existing = await this.repo.findOneBy({ alertId: message.alertId });
    if (existing) {
      this.logger.debug(`alert ${message.alertId} already ingested, skipping`);
      return existing;
    }

    const alert = this.repo.create({
      alertId: message.alertId,
      txnId: message.txnId,
      customerId: message.customerId,
      timestamp: new Date(message.timestamp),
      score: message.score,
      severity: message.severity,
      triggeredRules: message.triggeredRules,
      topFeatures: message.topFeatures,
      trailingMean: message.trailingMean,
      trailingMax: message.trailingMax,
      amount: message.amount,
      channel: message.channel,
      merchantId: message.merchantId,
      merchantCategory: message.merchantCategory,
      city: message.city,
      lat: message.lat,
      lon: message.lon,
      dispositionStatus: DispositionStatus.PENDING,
    });
    return this.repo.save(alert);
  }

  async findAll(query: ListAlertsDto): Promise<{ items: Alert[]; total: number }> {
    const qb = this.repo.createQueryBuilder('alert').orderBy('alert.timestamp', 'DESC');

    if (query.severity) qb.andWhere('alert.severity = :severity', { severity: query.severity });
    if (query.dispositionStatus) qb.andWhere('alert.dispositionStatus = :status', { status: query.dispositionStatus });
    if (query.customerId) qb.andWhere('alert.customerId = :customerId', { customerId: query.customerId });

    qb.take(query.limit).skip(query.offset);
    const [items, total] = await qb.getManyAndCount();
    return { items, total };
  }

  async findOne(alertId: string): Promise<Alert> {
    const alert = await this.repo.findOneBy({ alertId });
    if (!alert) throw new NotFoundException(`alert ${alertId} not found`);
    return alert;
  }

  /** A customer's other recent alerts, for drill-down context -- the
   * closest this service can offer to "the customer's last 20
   * transactions" without a queryable transaction history store
   * (that's the Delta Lake bronze layer's job, not this service's). */
  async findRecentForCustomer(customerId: string, excludeAlertId: string, limit = 20): Promise<Alert[]> {
    return this.repo.find({
      where: { customerId },
      order: { timestamp: 'DESC' },
      take: limit + 1,
    }).then((rows) => rows.filter((r) => r.alertId !== excludeAlertId).slice(0, limit));
  }

  async disposition(alertId: string, dto: CreateDispositionDto): Promise<DispositionResult> {
    const alert = await this.findOne(alertId);
    alert.dispositionStatus = dto.status;
    alert.dispositionNote = dto.note ?? null;
    alert.dispositionedAt = new Date();
    const saved = await this.repo.save(alert);

    return {
      alert: saved,
      dispositionEvent: {
        alertId: saved.alertId,
        customerId: saved.customerId,
        status: saved.dispositionStatus,
        note: saved.dispositionNote,
        dispositionedAt: saved.dispositionedAt!.toISOString(),
      },
    };
  }
}
