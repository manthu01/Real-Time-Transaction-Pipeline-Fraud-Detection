import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AlertsModule } from './alerts/alerts.module';
import { env } from './config/env';
import { KafkaConsumerModule } from './kafka/kafka-consumer.module';
import { MetricsModule } from './metrics/metrics.module';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: env.postgres.host,
      port: env.postgres.port,
      username: env.postgres.username,
      password: env.postgres.password,
      database: env.postgres.database,
      autoLoadEntities: true,
      // fine for this project's scale and the fact that the schema is
      // small and stable; a production deployment would use migrations.
      synchronize: true,
    }),
    AlertsModule,
    KafkaConsumerModule,
    MetricsModule,
  ],
})
export class AppModule {}
