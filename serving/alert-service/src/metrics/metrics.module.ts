import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Alert } from '../alerts/alert.entity';
import { MetricsController } from './metrics.controller';
import { MetricsService } from './metrics.service';

@Module({
  imports: [TypeOrmModule.forFeature([Alert])],
  controllers: [MetricsController],
  providers: [MetricsService],
})
export class MetricsModule {}
