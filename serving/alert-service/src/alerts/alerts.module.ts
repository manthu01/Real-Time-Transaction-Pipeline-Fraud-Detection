import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { KafkaProducerModule } from '../kafka/kafka-producer.module';
import { Alert } from './alert.entity';
import { AlertsController } from './alerts.controller';
import { AlertsGateway } from './alerts.gateway';
import { AlertsService } from './alerts.service';

@Module({
  imports: [TypeOrmModule.forFeature([Alert]), KafkaProducerModule],
  controllers: [AlertsController],
  providers: [AlertsService, AlertsGateway],
  exports: [AlertsService, AlertsGateway],
})
export class AlertsModule {}
