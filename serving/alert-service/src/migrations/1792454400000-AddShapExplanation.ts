import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Columns for the LightGBM champion's explanation and the Isolation
 * Forest shadow score. All nullable, no back-fill: existing rows were
 * scored by the Isolation Forest and have no SHAP values to recover, and
 * null is how the console recognises them and renders the legacy
 * z-score view. topFeatures needs no DDL -- it's jsonb, and its element
 * shape changing from {name, value, zScore} to {name, value, shap,
 * direction} is handled by readers, not by the schema.
 */
export class AddShapExplanation1792454400000 implements MigrationInterface {
  name = 'AddShapExplanation1792454400000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "alerts" ADD COLUMN IF NOT EXISTS "baseValue" double precision`);
    await queryRunner.query(`ALTER TABLE "alerts" ADD COLUMN IF NOT EXISTS "rawScore" double precision`);
    await queryRunner.query(`ALTER TABLE "alerts" ADD COLUMN IF NOT EXISTS "isoScore" double precision`);
    await queryRunner.query(`ALTER TABLE "alerts" ADD COLUMN IF NOT EXISTS "unusualFeatures" jsonb`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "alerts" DROP COLUMN "unusualFeatures"`);
    await queryRunner.query(`ALTER TABLE "alerts" DROP COLUMN "isoScore"`);
    await queryRunner.query(`ALTER TABLE "alerts" DROP COLUMN "rawScore"`);
    await queryRunner.query(`ALTER TABLE "alerts" DROP COLUMN "baseValue"`);
  }
}
