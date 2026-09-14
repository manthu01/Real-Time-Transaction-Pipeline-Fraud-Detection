import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { env } from '../config/env';

/** Publishes analyst disposition decisions to the compacted
 * analyst.dispositions topic -- the only source of ground-truth labels in
 * a real fraud operation, and the input a future retraining job consumes.
 * Keyed by alertId so the compacted topic retains one row per alert
 * (a later re-disposition overwrites the earlier one on compaction). */
@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly kafka = new Kafka({ clientId: env.kafka.clientId, brokers: env.kafka.brokers });
  private producer: Producer;

  async onModuleInit(): Promise<void> {
    this.producer = this.kafka.producer();
    await this.producer.connect();
    this.logger.log('Kafka producer connected');
  }

  async onModuleDestroy(): Promise<void> {
    await this.producer?.disconnect();
  }

  async publishDisposition(event: {
    alertId: string;
    customerId: string;
    status: string;
    note: string | null;
    dispositionedAt: string;
  }): Promise<void> {
    try {
      await this.producer.send({
        topic: env.kafka.dispositionsTopic,
        messages: [{ key: event.alertId, value: JSON.stringify(event) }],
      });
    } catch (err) {
      this.logger.error(`failed to publish disposition for ${event.alertId}`, err as Error);
    }
  }
}
