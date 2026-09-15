'use client';

import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { listAlerts } from '@/lib/api';
import { getSocket } from '@/lib/socket';
import type { Alert } from '@/lib/types';
import { DispositionBadge, SeverityBadge } from '@/components/Badges';

const MAX_FEED_SIZE = 200;

export default function LiveFeedPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [severityFilter, setSeverityFilter] = useState('');
  const [connected, setConnected] = useState(false);
  const newIds = useRef<Set<string>>(new Set());

  useEffect(() => {
    listAlerts({ limit: 100 })
      .then((res) => setAlerts(res.items))
      .catch((err) => setError(err instanceof Error ? err.message : 'failed to load alerts'))
      .finally(() => setLoading(false));

    const socket = getSocket();
    const onConnect = () => setConnected(true);
    const onDisconnect = () => setConnected(false);

    const onCreated = (alert: Alert) => {
      newIds.current.add(alert.alertId);
      setAlerts((prev) => [alert, ...prev].slice(0, MAX_FEED_SIZE));
    };
    const onUpdated = (alert: Alert) => {
      setAlerts((prev) => prev.map((a) => (a.alertId === alert.alertId ? alert : a)));
    };

    socket.on('connect', onConnect);
    socket.on('disconnect', onDisconnect);
    socket.on('alert.created', onCreated);
    socket.on('alert.updated', onUpdated);
    setConnected(socket.connected);

    return () => {
      socket.off('connect', onConnect);
      socket.off('disconnect', onDisconnect);
      socket.off('alert.created', onCreated);
      socket.off('alert.updated', onUpdated);
    };
  }, []);

  const visible = severityFilter ? alerts.filter((a) => a.severity === severityFilter) : alerts;

  return (
    <div className="container">
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <h2 style={{ margin: 0 }}>Live Feed</h2>
          <span className="muted">
            <span style={{ color: connected ? 'var(--confirmed)' : 'var(--critical)' }}>●</span>{' '}
            {connected ? 'live' : 'disconnected'}
          </span>
        </div>

        <div style={{ marginBottom: 12 }}>
          <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value)}>
            <option value="">All severities</option>
            <option value="CRITICAL">Critical</option>
            <option value="HIGH">High</option>
            <option value="MEDIUM">Medium</option>
            <option value="LOW">Low</option>
          </select>
        </div>

        {loading && <p className="muted">Loading…</p>}
        {error && <p style={{ color: 'var(--critical)' }}>{error}</p>}

        {!loading && !error && (
          <table>
            <thead>
              <tr>
                <th>Severity</th>
                <th>Time</th>
                <th>Customer</th>
                <th>Amount</th>
                <th>Score</th>
                <th>Rules</th>
                <th>Disposition</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((alert) => {
                const isNew = newIds.current.has(alert.alertId);
                return (
                  <tr
                    key={alert.alertId}
                    className={`alert-row ${isNew ? 'is-new' : ''}`}
                    onClick={() => (window.location.href = `/alerts/${alert.alertId}`)}
                  >
                    <td>
                      <SeverityBadge severity={alert.severity} />
                    </td>
                    <td className="muted">{new Date(alert.timestamp).toLocaleString()}</td>
                    <td>
                      <Link href={`/alerts/${alert.alertId}`} onClick={(e) => e.stopPropagation()}>
                        {alert.customerId}
                      </Link>
                    </td>
                    <td>{alert.amount != null ? `₹${alert.amount.toFixed(2)}` : '—'}</td>
                    <td>{alert.score.toFixed(3)}</td>
                    <td className="muted">{alert.triggeredRules.length > 0 ? alert.triggeredRules.join(', ') : 'model only'}</td>
                    <td>
                      <DispositionBadge status={alert.dispositionStatus} />
                    </td>
                  </tr>
                );
              })}
              {visible.length === 0 && (
                <tr>
                  <td colSpan={7} className="muted">
                    No alerts match this filter yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
