'use client';

import Link from 'next/link';
import { useParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { getAlert } from '@/lib/api';
import { getSocket } from '@/lib/socket';
import type { Alert } from '@/lib/types';
import { DispositionBadge, SeverityBadge } from '@/components/Badges';
import { DispositionForm } from '@/components/DispositionForm';
import { GeoEmbed } from '@/components/GeoEmbed';
import { TopFeaturesBars } from '@/components/TopFeaturesBars';

export default function AlertDetailPage() {
  // useParams() rather than a `params` prop: in Next.js 15, a page's
  // `params` prop is a Promise intended for (async) Server Components --
  // this page is a Client Component (it needs the WebSocket connection
  // and the disposition form's interactivity), and useParams() reads the
  // resolved route params synchronously from client router state instead.
  const params = useParams<{ alertId: string }>();
  const alertId = params.alertId;

  const [alert, setAlert] = useState<Alert | null>(null);
  const [recent, setRecent] = useState<Alert[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getAlert(alertId)
      .then((res) => {
        setAlert(res.alert);
        setRecent(res.recentForCustomer);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'failed to load alert'));

    const socket = getSocket();
    const onUpdated = (updated: Alert) => {
      if (updated.alertId === alertId) setAlert(updated);
    };
    socket.on('alert.updated', onUpdated);
    return () => {
      socket.off('alert.updated', onUpdated);
    };
  }, [alertId]);

  if (error) return <div className="container">
    <p style={{ color: 'var(--critical)' }}>{error}</p>
  </div>;
  if (!alert) return <div className="container"><p className="muted">Loading…</p></div>;

  return (
    <div className="container">
      <p style={{ marginBottom: 12 }}>
        <Link href="/">&larr; Live Feed</Link>
      </p>

      <div className="panel">
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 12 }}>
          <SeverityBadge severity={alert.severity} />
          <DispositionBadge status={alert.dispositionStatus} />
          <span className="muted">alert {alert.alertId}</span>
        </div>

        <dl className="kv">
          <dt>Customer</dt>
          <dd>{alert.customerId}</dd>
          <dt>Transaction</dt>
          <dd>{alert.txnId}</dd>
          <dt>Time</dt>
          <dd>{new Date(alert.timestamp).toLocaleString()}</dd>
          <dt>Amount</dt>
          <dd>{alert.amount != null ? `₹${alert.amount.toFixed(2)}` : '—'}</dd>
          <dt>Channel</dt>
          <dd>{alert.channel ?? '—'}</dd>
          <dt>Merchant</dt>
          <dd>
            {alert.merchantId ?? '—'} ({alert.merchantCategory ?? '—'})
          </dd>
          <dt>Anomaly score</dt>
          <dd>{alert.score.toFixed(4)}</dd>
          <dt>Customer trailing mean</dt>
          <dd>{alert.trailingMean != null ? `₹${alert.trailingMean.toFixed(2)}` : '—'}</dd>
        </dl>
      </div>

      <div className="panel">
        <h2>Why this fired</h2>
        {alert.triggeredRules.length > 0 ? (
          <p>
            Triggered rules:{' '}
            {alert.triggeredRules.map((r) => (
              <span key={r} className="badge badge-high" style={{ marginRight: 6 }}>
                {r}
              </span>
            ))}
          </p>
        ) : (
          <p className="muted">No rule fired — flagged by the anomaly model alone.</p>
        )}
        <h3 style={{ fontSize: 13, color: 'var(--text-dim)', textTransform: 'uppercase', marginTop: 16 }}>
          Top contributing features
        </h3>
        <TopFeaturesBars features={alert.topFeatures} />
      </div>

      {alert.lat != null && alert.lon != null && (
        <div className="panel">
          <h2>Location</h2>
          <GeoEmbed lat={alert.lat} lon={alert.lon} label={alert.city ?? 'transaction location'} />
        </div>
      )}

      <div className="panel">
        <h2>Customer&apos;s other recent alerts</h2>
        {recent.length === 0 ? (
          <p className="muted">No other recent alerts for this customer.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Severity</th>
                <th>Disposition</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((r) => (
                <tr key={r.alertId} className="alert-row" onClick={() => (window.location.href = `/alerts/${r.alertId}`)}>
                  <td className="muted">{new Date(r.timestamp).toLocaleString()}</td>
                  <td>
                    <SeverityBadge severity={r.severity} />
                  </td>
                  <td>
                    <DispositionBadge status={r.dispositionStatus} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="panel">
        <h2>Disposition</h2>
        <DispositionForm alert={alert} onDispositioned={setAlert} />
      </div>
    </div>
  );
}
