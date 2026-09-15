import type { DispositionStatus, Severity } from '@/lib/types';

export function SeverityBadge({ severity }: { severity: Severity | string }) {
  return <span className={`badge badge-${severity.toLowerCase()}`}>{severity}</span>;
}

export function DispositionBadge({ status }: { status: DispositionStatus | string }) {
  const label = status.replace(/_/g, ' ');
  return <span className={`badge badge-${status.toLowerCase()}`}>{label}</span>;
}
