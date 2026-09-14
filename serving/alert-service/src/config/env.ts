/** Small, dependency-free env accessor -- avoids pulling in @nestjs/config
 * for a handful of settings that only ever come from process.env or a
 * documented default. */
export const env = {
  port: parseInt(process.env.PORT ?? '3001', 10),
  postgres: {
    host: process.env.POSTGRES_HOST ?? 'localhost',
    port: parseInt(process.env.POSTGRES_PORT ?? '5432', 10),
    username: process.env.POSTGRES_USER ?? 'pulseguard',
    password: process.env.POSTGRES_PASSWORD ?? 'pulseguard',
    database: process.env.POSTGRES_DB ?? 'pulseguard',
  },
  kafka: {
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    alertsTopic: process.env.KAFKA_ALERTS_TOPIC ?? 'fraud.alerts',
    dispositionsTopic: process.env.KAFKA_DISPOSITIONS_TOPIC ?? 'analyst.dispositions',
    clientId: process.env.KAFKA_CLIENT_ID ?? 'pulseguard-alert-service',
    consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID ?? 'pulseguard-alert-service',
  },
  corsOrigin: process.env.CORS_ORIGIN ?? '*',
};
