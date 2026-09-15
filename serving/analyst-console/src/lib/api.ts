import type { AlertDetailResponse, AlertListResponse, DispositionStatus, MetricsSummary } from './types';

// NEXT_PUBLIC_* so it's available in client components (the whole console
// is client-rendered against a REST/WebSocket API, not server-rendered
// from this env var) -- falls back to the docker-compose host mapping for
// local dev without a .env.local.
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? 'http://localhost:3001';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    cache: 'no-store',
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`${init?.method ?? 'GET'} ${path} failed: ${res.status} ${body}`);
  }
  return res.json() as Promise<T>;
}

export interface ListAlertsParams {
  limit?: number;
  offset?: number;
  severity?: string;
  dispositionStatus?: string;
  customerId?: string;
}

export function listAlerts(params: ListAlertsParams = {}): Promise<AlertListResponse> {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') qs.set(key, String(value));
  }
  const query = qs.toString();
  return request<AlertListResponse>(`/alerts${query ? `?${query}` : ''}`);
}

export function getAlert(alertId: string): Promise<AlertDetailResponse> {
  return request<AlertDetailResponse>(`/alerts/${alertId}`);
}

export function submitDisposition(alertId: string, status: DispositionStatus, note?: string) {
  return request(`/alerts/${alertId}/disposition`, {
    method: 'POST',
    body: JSON.stringify({ status, note }),
  });
}

export function getMetricsSummary(): Promise<MetricsSummary> {
  return request<MetricsSummary>('/metrics/summary');
}
