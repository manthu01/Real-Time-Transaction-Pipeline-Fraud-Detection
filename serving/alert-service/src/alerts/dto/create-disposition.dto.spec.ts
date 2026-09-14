import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { DispositionStatus } from '../alert.entity';
import { CreateDispositionDto } from './create-disposition.dto';

async function errorsFor(payload: Record<string, unknown>) {
  const dto = plainToInstance(CreateDispositionDto, payload);
  return validate(dto);
}

describe('CreateDispositionDto', () => {
  it('accepts a valid status with no note', async () => {
    const errors = await errorsFor({ status: DispositionStatus.CONFIRMED_FRAUD });
    expect(errors).toHaveLength(0);
  });

  it('accepts a valid status with a note', async () => {
    const errors = await errorsFor({ status: DispositionStatus.FALSE_POSITIVE, note: 'looked like the customer travelling' });
    expect(errors).toHaveLength(0);
  });

  it('rejects an arbitrary status string', async () => {
    const errors = await errorsFor({ status: 'MAYBE_FRAUD' });
    expect(errors.length).toBeGreaterThan(0);
  });

  it('rejects a note over 2000 characters', async () => {
    const errors = await errorsFor({ status: DispositionStatus.NEEDS_INVESTIGATION, note: 'x'.repeat(2001) });
    expect(errors.length).toBeGreaterThan(0);
  });
});
