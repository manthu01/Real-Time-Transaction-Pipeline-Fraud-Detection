import { Logger } from '@nestjs/common';
import { OnGatewayConnection, OnGatewayDisconnect, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { env } from '../config/env';
import { Alert } from './alert.entity';

/** Pushes alert lifecycle events to the analyst console's live feed.
 * Two event types only: a new alert arriving from Kafka, and an existing
 * alert's disposition changing -- everything the console needs to keep
 * its feed and any open drill-down in sync without polling. */
@WebSocketGateway({ cors: { origin: env.corsOrigin } })
export class AlertsGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(AlertsGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`analyst console connected: ${client.id}`);
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`analyst console disconnected: ${client.id}`);
  }

  emitAlertCreated(alert: Alert): void {
    this.server?.emit('alert.created', alert);
  }

  emitAlertUpdated(alert: Alert): void {
    this.server?.emit('alert.updated', alert);
  }
}
