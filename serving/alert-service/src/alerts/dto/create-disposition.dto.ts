import { IsEnum, IsOptional, IsString, MaxLength } from 'class-validator';
import { DispositionStatus } from '../alert.entity';

export class CreateDispositionDto {
  @IsEnum(DispositionStatus, { message: 'status must be one of CONFIRMED_FRAUD, FALSE_POSITIVE, NEEDS_INVESTIGATION' })
  status: DispositionStatus;

  @IsOptional()
  @IsString()
  @MaxLength(2000)
  note?: string;
}
