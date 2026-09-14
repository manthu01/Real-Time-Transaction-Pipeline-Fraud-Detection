import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Alert } from '../alerts/alert.entity';
import { MetricsService } from './metrics.service';

describe('MetricsService', () => {
  let service: MetricsService;
  let selectArg: string;
  let mockQueryBuilder: Record<string, jest.Mock>;

  beforeEach(async () => {
    mockQueryBuilder = {
      select: jest.fn((expr: string) => {
        selectArg = expr;
        return mockQueryBuilder;
      }),
      where: jest.fn().mockReturnThis(),
      getRawOne: jest.fn().mockResolvedValue({ avgSeconds: '120.5' }),
    };
    const repo = { createQueryBuilder: jest.fn(() => mockQueryBuilder) };

    const module: TestingModule = await Test.createTestingModule({
      providers: [MetricsService, { provide: getRepositoryToken(Alert), useValue: repo }],
    }).compile();
    service = module.get(MetricsService);
  });

  describe('meanTimeToDispositionSeconds', () => {
    it('measures from ingestedAt, not the transaction event-time timestamp', async () => {
      // Regression test: this previously used alert.timestamp (the
      // underlying transaction's event time), which for backfilled data
      // can be arbitrarily far in the past and produced a nonsensical
      // (observed: negative) "time to disposition" against live data.
      await service.meanTimeToDispositionSeconds();

      expect(selectArg).toContain('"ingestedAt"');
      expect(selectArg).not.toContain('alert.timestamp');
    });

    it('returns the parsed average when present', async () => {
      const result = await service.meanTimeToDispositionSeconds();
      expect(result).toBe(120.5);
    });

    it('returns null when there are no dispositioned alerts yet', async () => {
      mockQueryBuilder.getRawOne = jest.fn().mockResolvedValue({ avgSeconds: null });
      const result = await service.meanTimeToDispositionSeconds();
      expect(result).toBeNull();
    });
  });
});
