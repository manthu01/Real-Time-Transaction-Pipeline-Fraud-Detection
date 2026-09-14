import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { AlertsGateway } from './alerts.gateway';
import { AlertsService } from './alerts.service';
import { CreateDispositionDto } from './dto/create-disposition.dto';
import { ListAlertsDto } from './dto/list-alerts.dto';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Controller('alerts')
export class AlertsController {
  constructor(
    private readonly alertsService: AlertsService,
    private readonly gateway: AlertsGateway,
    private readonly producer: KafkaProducerService,
  ) {}

  @Get()
  findAll(@Query() query: ListAlertsDto) {
    return this.alertsService.findAll(query);
  }

  @Get(':alertId')
  async findOne(@Param('alertId') alertId: string) {
    const alert = await this.alertsService.findOne(alertId);
    const recentForCustomer = await this.alertsService.findRecentForCustomer(alert.customerId, alertId);
    return { alert, recentForCustomer };
  }

  @Post(':alertId/disposition')
  async disposition(@Param('alertId') alertId: string, @Body() dto: CreateDispositionDto) {
    const result = await this.alertsService.disposition(alertId, dto);
    this.gateway.emitAlertUpdated(result.alert);
    // fire-and-forget from the HTTP response's perspective, but errors are
    // logged inside the producer rather than silently swallowed
    await this.producer.publishDisposition(result.dispositionEvent);
    return result.alert;
  }
}
