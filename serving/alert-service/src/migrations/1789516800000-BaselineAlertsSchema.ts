import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * The alerts schema as TypeORM's `synchronize` created it before this
 * service switched to migrations -- statement for statement, including
 * TypeORM's generated constraint/index names (captured from
 * `createSchemaBuilder().log()` against an empty database).
 *
 * Every statement is IF NOT EXISTS, so this is a no-op on a database the
 * old synchronize-based service already created (it gets recorded as
 * applied and nothing else happens), and it builds the identical schema
 * on a fresh one. Reusing the generated index names is what makes the
 * no-op true: different names would quietly create a second copy of
 * every index on existing databases.
 */
export class BaselineAlertsSchema1789516800000 implements MigrationInterface {
  name = 'BaselineAlertsSchema1789516800000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      DO $$ BEGIN
        CREATE TYPE "public"."alerts_dispositionstatus_enum" AS ENUM('PENDING', 'CONFIRMED_FRAUD', 'FALSE_POSITIVE', 'NEEDS_INVESTIGATION');
      EXCEPTION WHEN duplicate_object THEN NULL;
      END $$
    `);
    await queryRunner.query(
      `CREATE TABLE IF NOT EXISTS "alerts" ("alertId" uuid NOT NULL, "txnId" character varying NOT NULL, "customerId" character varying NOT NULL, "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL, "score" double precision NOT NULL, "severity" character varying NOT NULL, "triggeredRules" jsonb NOT NULL DEFAULT '[]', "topFeatures" jsonb NOT NULL DEFAULT '[]', "trailingMean" double precision, "trailingMax" double precision, "amount" double precision, "channel" character varying, "merchantId" character varying, "merchantCategory" character varying, "city" character varying, "lat" double precision, "lon" double precision, "dispositionStatus" "public"."alerts_dispositionstatus_enum" NOT NULL DEFAULT 'PENDING', "dispositionNote" text, "dispositionedAt" TIMESTAMP WITH TIME ZONE, "ingestedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_0c6d99761f623e4659169491f20" PRIMARY KEY ("alertId"))`,
    );
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "IDX_8cecd75bc2b8b2ebd92851f2a6" ON "alerts" ("txnId")`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "IDX_0f709556b2356043ff74a1b459" ON "alerts" ("customerId")`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "IDX_bd39bea0ff020151ca975491db" ON "alerts" ("timestamp")`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "IDX_f206618be4e26b7c883e9899ba" ON "alerts" ("severity")`);
    await queryRunner.query(`CREATE INDEX IF NOT EXISTS "IDX_80af2d57b8e12ce574f98ded4e" ON "alerts" ("dispositionStatus")`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE "alerts"`);
    await queryRunner.query(`DROP TYPE "public"."alerts_dispositionstatus_enum"`);
  }
}
