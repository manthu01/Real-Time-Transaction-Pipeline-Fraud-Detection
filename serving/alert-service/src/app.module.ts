import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AlertsModule } from './alerts/alerts.module';
import { env } from './config/env';
import { KafkaConsumerModule } from './kafka/kafka-consumer.module';
import { MetricsModule } from './metrics/metrics.module';
import { MIGRATIONS } from './migrations';

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
      // Schema changes go through migrations (src/migrations), applied at
      // startup. This used to be `synchronize: true`; the first migration
      // reproduces exactly what synchronize created, so databases from
      // before the switch upgrade in place without losing rows.
      synchronize: false,
      migrations: MIGRATIONS,
      migrationsRun: true,
    }),
    AlertsModule,
    KafkaConsumerModule,
    MetricsModule,
  ],
})
export class AppModule {}
