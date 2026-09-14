import { NotFoundException } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Alert, DispositionStatus } from './alert.entity';
import { AlertsService } from './alerts.service';

type MockRepo = Partial<Record<keyof Repository<Alert>, jest.Mock>>;

const createMockRepo = (): MockRepo => ({
  findOneBy: jest.fn(),
  create: jest.fn((data) => data as Alert),
  save: jest.fn((data) => Promise.resolve({ ...data })),
  find: jest.fn(),
  createQueryBuilder: jest.fn(),
});

const KAFKA_MESSAGE = {
  alertId: '11111111-1111-1111-1111-111111111111',
  txnId: 'txn-1',
  customerId: 'C000001',
  timestamp: '2026-03-10T10:00:00Z',
  score: 0.87,
  severity: 'HIGH',
  triggeredRules: ['IMPOSSIBLE_TRAVEL'],
  topFeatures: [{ name: 'impliedSpeedKmh', value: 3000, zScore: 12.4 }],
  trailingMean: 500,
  trailingMax: 2000,
};

describe('AlertsService', () => {
  let service: AlertsService;
  let repo: MockRepo;

  beforeEach(async () => {
    repo = createMockRepo();
    const module: TestingModule = await Test.createTestingModule({
      providers: [AlertsService, { provide: getRepositoryToken(Alert), useValue: repo }],
    }).compile();
    service = module.get(AlertsService);
  });

  describe('ingestFromKafka', () => {
    it('creates a new alert with PENDING disposition when not already ingested', async () => {
      (repo.findOneBy as jest.Mock).mockResolvedValue(null);

      const alert = await service.ingestFromKafka(KAFKA_MESSAGE);

      expect(repo.create).toHaveBeenCalledWith(
        expect.objectContaining({ alertId: KAFKA_MESSAGE.alertId, dispositionStatus: DispositionStatus.PENDING }),
      );
      expect(alert.dispositionStatus).toBe(DispositionStatus.PENDING);
    });

    it('is idempotent: re-ingesting the same alertId does not create a duplicate', async () => {
      const existing = { alertId: KAFKA_MESSAGE.alertId } as Alert;
      (repo.findOneBy as jest.Mock).mockResolvedValue(existing);

      const result = await service.ingestFromKafka(KAFKA_MESSAGE);

      expect(repo.create).not.toHaveBeenCalled();
      expect(repo.save).not.toHaveBeenCalled();
      expect(result).toBe(existing);
    });
  });

  describe('findOne', () => {
    it('throws NotFoundException when the alert does not exist', async () => {
      (repo.findOneBy as jest.Mock).mockResolvedValue(null);
      await expect(service.findOne('does-not-exist')).rejects.toBeInstanceOf(NotFoundException);
    });
  });

  describe('disposition', () => {
    it('sets status, note, and dispositionedAt, and returns a matching Kafka event payload', async () => {
      const alert = {
        alertId: KAFKA_MESSAGE.alertId,
        customerId: KAFKA_MESSAGE.customerId,
        dispositionStatus: DispositionStatus.PENDING,
        dispositionNote: null,
        dispositionedAt: null,
      } as Alert;
      (repo.findOneBy as jest.Mock).mockResolvedValue(alert);

      const result = await service.disposition(KAFKA_MESSAGE.alertId, {
        status: DispositionStatus.CONFIRMED_FRAUD,
        note: 'confirmed with customer',
      });

      expect(result.alert.dispositionStatus).toBe(DispositionStatus.CONFIRMED_FRAUD);
      expect(result.alert.dispositionNote).toBe('confirmed with customer');
      expect(result.alert.dispositionedAt).toBeInstanceOf(Date);
      expect(result.dispositionEvent).toEqual(
        expect.objectContaining({
          alertId: KAFKA_MESSAGE.alertId,
          customerId: KAFKA_MESSAGE.customerId,
          status: DispositionStatus.CONFIRMED_FRAUD,
          note: 'confirmed with customer',
        }),
      );
    });

    it('defaults note to null when not provided', async () => {
      const alert = {
        alertId: KAFKA_MESSAGE.alertId,
        customerId: KAFKA_MESSAGE.customerId,
        dispositionStatus: DispositionStatus.PENDING,
      } as Alert;
      (repo.findOneBy as jest.Mock).mockResolvedValue(alert);

      const result = await service.disposition(KAFKA_MESSAGE.alertId, { status: DispositionStatus.FALSE_POSITIVE });

      expect(result.alert.dispositionNote).toBeNull();
    });
  });

  describe('findRecentForCustomer', () => {
    it('excludes the current alert and caps at the requested limit', async () => {
      const rows = Array.from({ length: 25 }, (_, i) => ({ alertId: `a${i}`, customerId: 'C1' }) as Alert);
      (repo.find as jest.Mock).mockResolvedValue([{ alertId: 'exclude-me', customerId: 'C1' } as Alert, ...rows]);

      const result = await service.findRecentForCustomer('C1', 'exclude-me', 20);

      expect(result).toHaveLength(20);
      expect(result.find((r) => r.alertId === 'exclude-me')).toBeUndefined();
    });
  });
});
