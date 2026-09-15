'use client';

import { useEffect, useState } from 'react';
import { getMetricsSummary } from '@/lib/api';
import type { MetricsSummary } from '@/lib/types';

function Sparkline({ values }: { values: number[] }) {
  const max = Math.max(...values, 1);
  return (
    <div className="sparkline-row">
      {values.map((v, i) => (
        <div key={i} className="sparkline-bar" style={{ height: `${Math.max((v / max) * 100, 2)}%` }} title={String(v)} />
      ))}
    </div>
  );
}

function formatSeconds(seconds: number | null): string {
  if (seconds == null) return '—';
  if (seconds < 60) return `${seconds.toFixed(0)}s`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

export default function MetricsPage() {
  const [metrics, setMetrics] = useState<MetricsSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getMetricsSummary()
      .then(setMetrics)
      .catch((err) => setError(err instanceof Error ? err.message : 'failed to load metrics'));
  }, []);

  if (error) return <div className="container"><p style={{ color: 'var(--critical)' }}>{error}</p></div>;
  if (!metrics) return <div className="container"><p className="muted">Loading…</p></div>;

  const totalAlerts = metrics.alertsPerDay.reduce((sum, d) => sum + d.count, 0);

  return (
    <div className="container">
      <div className="panel">
        <h2>Summary</h2>
        <div className="stat-grid">
          <div className="stat">
            <div className="value">{totalAlerts.toLocaleString()}</div>
            <div className="label">Alerts (recent days)</div>
          </div>
          <div className="stat">
            <div className="value">{formatSeconds(metrics.meanTimeToDispositionSeconds)}</div>
            <div className="label">Mean time to disposition</div>
          </div>
          <div className="stat">
            <div className="value">{metrics.precisionByRule.length}</div>
            <div className="label">Rules with dispositioned alerts</div>
          </div>
        </div>
      </div>

      <div className="panel">
        <h2>Alerts per day</h2>
        {metrics.alertsPerDay.length === 0 ? (
          <p className="muted">No alerts yet.</p>
        ) : (
          <>
            <Sparkline values={metrics.alertsPerDay.map((d) => d.count)} />
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 6 }} className="muted">
              <span>{metrics.alertsPerDay[0].day}</span>
              <span>{metrics.alertsPerDay[metrics.alertsPerDay.length - 1].day}</span>
            </div>
          </>
        )}
      </div>

      <div className="panel">
        <h2>Precision by rule</h2>
        <p className="muted" style={{ marginBottom: 10 }}>
          Of the alerts each rule fired on that an analyst has since dispositioned, what fraction were confirmed
          fraud. A rule with high volume and low precision is burning analyst time.
        </p>
        {metrics.precisionByRule.length === 0 ? (
          <p className="muted">No dispositioned rule-triggered alerts yet.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Rule</th>
                <th>Confirmed fraud</th>
                <th>False positive</th>
                <th>Precision</th>
              </tr>
            </thead>
            <tbody>
              {metrics.precisionByRule.map((r) => (
                <tr key={r.rule}>
                  <td>{r.rule}</td>
                  <td>{r.confirmedFraud}</td>
                  <td>{r.falsePositive}</td>
                  <td>{r.precision != null ? `${(r.precision * 100).toFixed(1)}%` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="panel">
        <h2>False-positive rate trend</h2>
        {metrics.falsePositiveRateTrend.length === 0 ? (
          <p className="muted">No dispositioned alerts yet.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Day</th>
                <th>Dispositioned</th>
                <th>False-positive rate</th>
              </tr>
            </thead>
            <tbody>
              {metrics.falsePositiveRateTrend.map((p) => (
                <tr key={p.day}>
                  <td>{p.day}</td>
                  <td>{p.dispositionedCount}</td>
                  <td>{p.falsePositiveRate != null ? `${(p.falsePositiveRate * 100).toFixed(1)}%` : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
