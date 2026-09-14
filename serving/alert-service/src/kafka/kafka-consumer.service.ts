import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { AlertsGateway } from '../alerts/alerts.gateway';
import { AlertsService } from '../alerts/alerts.service';
import { env } from '../config/env';
import { isValidKafkaAlertMessage } from './kafka-alert-message';

/** Consumes fraud.alerts, persists each alert, and pushes it to any
 * connected analyst console over WebSocket. A malformed message is
 * logged and skipped rather than crashing the consumer -- the same
 * "don't let one bad record take down the pipeline" principle as the
 * Spark job's dead-letter routing, just without a dead-letter topic of
 * its own since a bad message here means a bug in the producer, not a
 * volume of untrusted external input worth a durable side channel. */
@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private readonly kafka = new Kafka({ clientId: env.kafka.clientId, brokers: env.kafka.brokers });
  private consumer: Consumer;

  constructor(
    private readonly alertsService: AlertsService,
    private readonly gateway: AlertsGateway,
  ) {}

  async onModuleInit(): Promise<void> {
    this.consumer = this.kafka.consumer({ groupId: env.kafka.consumerGroupId });
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: env.kafka.alertsTopic, fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        let parsed: unknown;
        try {
          parsed = JSON.parse(message.value.toString());
        } catch (err) {
          this.logger.warn(`skipping unparseable message on ${env.kafka.alertsTopic}: ${(err as Error).message}`);
          return;
        }
        if (!isValidKafkaAlertMessage(parsed)) {
          this.logger.warn(`skipping malformed alert message: ${JSON.stringify(parsed)}`);
          return;
        }
        const alert = await this.alertsService.ingestFromKafka(parsed);
        this.gateway.emitAlertCreated(alert);
      },
    });
    this.logger.log(`Kafka consumer subscribed to ${env.kafka.alertsTopic}`);
  }

  async onModuleDestroy(): Promise<void> {
    await this.consumer?.disconnect();
  }
}
