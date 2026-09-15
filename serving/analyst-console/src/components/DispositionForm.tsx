'use client';

import { useState } from 'react';
import { submitDisposition } from '@/lib/api';
import type { Alert, DispositionStatus } from '@/lib/types';

const OPTIONS: { status: DispositionStatus; label: string; className: string }[] = [
  { status: 'CONFIRMED_FRAUD', label: 'Confirmed Fraud', className: 'confirmed_fraud' },
  { status: 'FALSE_POSITIVE', label: 'False Positive', className: 'false_positive' },
  { status: 'NEEDS_INVESTIGATION', label: 'Needs Investigation', className: 'needs_investigation' },
];

export function DispositionForm({ alert, onDispositioned }: { alert: Alert; onDispositioned: (updated: Alert) => void }) {
  const [note, setNote] = useState('');
  const [submitting, setSubmitting] = useState<DispositionStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  const alreadyDispositioned = alert.dispositionStatus !== 'PENDING';

  async function handleClick(status: DispositionStatus) {
    setSubmitting(status);
    setError(null);
    try {
      const updated = (await submitDisposition(alert.alertId, status, note || undefined)) as Alert;
      onDispositioned(updated);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'failed to submit disposition');
    } finally {
      setSubmitting(null);
    }
  }

  return (
    <div>
      {alreadyDispositioned && (
        <p className="muted">
          Already dispositioned{alert.dispositionedAt ? ` at ${new Date(alert.dispositionedAt).toLocaleString()}` : ''}. Submitting again
          overwrites the previous decision.
        </p>
      )}
      <textarea
        placeholder="Note (optional) -- what did you check?"
        value={note}
        onChange={(e) => setNote(e.target.value)}
        maxLength={2000}
      />
      <div className="disposition-buttons">
        {OPTIONS.map((opt) => (
          <button
            key={opt.status}
            className={`disposition-btn ${opt.className}`}
            disabled={submitting !== null}
            onClick={() => handleClick(opt.status)}
          >
            {submitting === opt.status ? 'Submitting…' : opt.label}
          </button>
        ))}
      </div>
      {error && <p style={{ color: 'var(--critical)', marginTop: 8 }}>{error}</p>}
    </div>
  );
}
