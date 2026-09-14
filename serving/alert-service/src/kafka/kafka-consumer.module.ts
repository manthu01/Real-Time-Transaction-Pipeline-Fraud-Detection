import { Module } from '@nestjs/common';
import { AlertsModule } from '../alerts/alerts.module';
import { KafkaConsumerService } from './kafka-consumer.service';

@Module({
  imports: [AlertsModule],
  providers: [KafkaConsumerService],
})
export class KafkaConsumerModule {}
