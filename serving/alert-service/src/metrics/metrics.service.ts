import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Alert, DispositionStatus } from '../alerts/alert.entity';

export interface AlertsPerDay {
  day: string;
  count: number;
}

export interface RulePrecision {
  rule: string;
  confirmedFraud: number;
  falsePositive: number;
  precision: number | null; // null when no dispositioned alerts yet for this rule
}

export interface FalsePositiveRatePoint {
  day: string;
  falsePositiveRate: number | null;
  dispositionedCount: number;
}

@Injectable()
export class MetricsService {
  constructor(@InjectRepository(Alert) private readonly repo: Repository<Alert>) {}

  async alertsPerDay(days = 14): Promise<AlertsPerDay[]> {
    const rows = await this.repo
      .createQueryBuilder('alert')
      .select("to_char(date_trunc('day', alert.timestamp), 'YYYY-MM-DD')", 'day')
      .addSelect('COUNT(*)', 'count')
      .where("alert.timestamp >= NOW() - (:days || ' days')::interval", { days })
      .groupBy('day')
      .orderBy('day', 'ASC')
      .getRawMany<{ day: string; count: string }>();
    return rows.map((r) => ({ day: r.day, count: parseInt(r.count, 10) }));
  }

  /** Per-rule precision: of the alerts a rule fired on that an analyst has
   * since dispositioned, what fraction were confirmed fraud? This is the
   * number that answers "is this rule worth keeping" -- a rule with high
   * volume and low precision is a rule burning analyst time. */
  async precisionByRule(): Promise<RulePrecision[]> {
    // TypeORM's QueryBuilder.innerJoin() expects a relation path or a
    // subquery function, not an arbitrary set-returning expression like
    // jsonb_array_elements_text() -- it mangles the generated SQL
    // ("zero-length delimited identifier"). The classic Postgres pattern
    // for unnesting a jsonb array per row (an implicit CROSS JOIN LATERAL
    // via a comma in FROM) is plain raw SQL instead.
    const rows: Array<{ rule: string; confirmedFraud: string; falsePositive: string }> = await this.repo.query(
      `SELECT rule,
              COUNT(*) FILTER (WHERE "dispositionStatus" = $1) AS "confirmedFraud",
              COUNT(*) FILTER (WHERE "dispositionStatus" = $2) AS "falsePositive"
       FROM alerts, jsonb_array_elements_text("triggeredRules") AS rule
       WHERE "dispositionStatus" IN ($1, $2)
       GROUP BY rule
       ORDER BY rule ASC`,
      [DispositionStatus.CONFIRMED_FRAUD, DispositionStatus.FALSE_POSITIVE],
    );

    return rows.map((r) => {
      const confirmed = parseInt(r.confirmedFraud, 10);
      const falsePos = parseInt(r.falsePositive, 10);
      const total = confirmed + falsePos;
      return {
        rule: r.rule,
        confirmedFraud: confirmed,
        falsePositive: falsePos,
        precision: total > 0 ? confirmed / total : null,
      };
    });
  }

  async meanTimeToDispositionSeconds(): Promise<number | null> {
    // Measured from ingestedAt (when the alert actually appeared in this
    // service), not the transaction's event-time `timestamp` -- that
    // field is when the underlying transaction happened, which for
    // backfilled/historical data can be arbitrarily far in the past and
    // produces a meaningless (even negative) duration. "Time to
    // disposition" is an analyst-workflow metric: how long an alert sat
    // in the queue before someone reviewed it.
    const result = await this.repo
      .createQueryBuilder('alert')
      .select('AVG(EXTRACT(EPOCH FROM (alert."dispositionedAt" - alert."ingestedAt")))', 'avgSeconds')
      .where('alert."dispositionedAt" IS NOT NULL')
      .getRawOne<{ avgSeconds: string | null }>();
    return result?.avgSeconds != null ? parseFloat(result.avgSeconds) : null;
  }

  async falsePositiveRateTrend(days = 14): Promise<FalsePositiveRatePoint[]> {
    const rows = await this.repo
      .createQueryBuilder('alert')
      .select("to_char(date_trunc('day', alert.\"dispositionedAt\"), 'YYYY-MM-DD')", 'day')
      .addSelect(`COUNT(*) FILTER (WHERE alert."dispositionStatus" = :falsePositive)`, 'falsePositive')
      .addSelect('COUNT(*)', 'dispositioned')
      .where('alert."dispositionedAt" >= NOW() - (:days || \' days\')::interval', { days })
      .andWhere('alert."dispositionStatus" IN (:...dispositioned)', {
        dispositioned: [DispositionStatus.CONFIRMED_FRAUD, DispositionStatus.FALSE_POSITIVE],
      })
      .setParameters({ falsePositive: DispositionStatus.FALSE_POSITIVE })
      .groupBy('day')
      .orderBy('day', 'ASC')
      .getRawMany<{ day: string; falsePositive: string; dispositioned: string }>();

    return rows.map((r) => {
      const dispositioned = parseInt(r.dispositioned, 10);
      const falsePositive = parseInt(r.falsePositive, 10);
      return {
        day: r.day,
        dispositionedCount: dispositioned,
        falsePositiveRate: dispositioned > 0 ? falsePositive / dispositioned : null,
      };
    });
  }

  async summary() {
    const [alertsPerDay, precisionByRule, meanTimeToDispositionSeconds, falsePositiveRateTrend] = await Promise.all([
      this.alertsPerDay(),
      this.precisionByRule(),
      this.meanTimeToDispositionSeconds(),
      this.falsePositiveRateTrend(),
    ]);
    return { alertsPerDay, precisionByRule, meanTimeToDispositionSeconds, falsePositiveRateTrend };
  }
}
