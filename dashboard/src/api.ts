import type { AuditEntry, DeviceRow, Health, Incident, Meta, Proposal } from "./types";

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(path, { headers: { "content-type": "application/json" }, ...init });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.detail ? String(body.detail) : `${r.status} ${r.statusText}`);
  }
  return r.json() as Promise<T>;
}

export const api = {
  health: () => req<Health>("/api/health"),
  meta: () => req<Meta>("/api/meta"),
  devices: () => req<DeviceRow[]>("/api/devices"),
  readings: (device: string, seconds = 60) =>
    req<{ ts: number; metric: string; value: number }[]>(`/api/devices/${device}/readings?seconds=${seconds}&limit=20000`),
  anomalies: (limit = 200) => req<import("./types").Anomaly[]>(`/api/anomalies?limit=${limit}`),
  incidents: (limit = 100) => req<Incident[]>(`/api/incidents?limit=${limit}`),
  proposals: (status?: string) => req<Proposal[]>(`/api/proposals${status ? `?status=${status}` : ""}`),
  audit: () => req<AuditEntry[]>("/api/audit?limit=300"),
  inject: (device: string, metric: string, kind: string) =>
    req(`/api/devices/${device}/faults`, { method: "POST", body: JSON.stringify({ metric, kind }) }),
  decide: (id: string, decision: "approve" | "reject", actor: string, note?: string) =>
    req<Proposal>(`/api/proposals/${id}/${decision}`, { method: "POST", body: JSON.stringify({ actor, note: note || null }) }),
};

export const fmtTime = (ts: number) => new Date(ts * 1000).toLocaleTimeString();
export const ago = (ts: number) => {
  const s = Math.max(0, Date.now() / 1000 - ts);
  return s < 60 ? `${s.toFixed(0)}s ago` : s < 3600 ? `${(s / 60).toFixed(0)}m ago` : `${(s / 3600).toFixed(1)}h ago`;
};
