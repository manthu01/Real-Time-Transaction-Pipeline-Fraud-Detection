import { Module } from '@nestjs/common';
import { KafkaProducerService } from './kafka-producer.service';

/** Split from the consumer into its own module so AlertsModule (which
 * needs the producer, to publish dispositions) doesn't end up in a
 * circular dependency with the consumer (which needs AlertsModule, to
 * persist ingested alerts). */
@Module({
  providers: [KafkaProducerService],
  exports: [KafkaProducerService],
})
export class KafkaProducerModule {}
