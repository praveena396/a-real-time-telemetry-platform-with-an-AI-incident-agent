export type Metric = "temperature" | "vibration" | "pressure";
export const METRICS: Metric[] = ["temperature", "vibration", "pressure"];
export const UNITS: Record<Metric, string> = { temperature: "°C", vibration: "mm/s", pressure: "kPa" };

export interface Reading { type: "reading"; ts: number; device_id: string; metric: Metric; value: number }
export interface Anomaly {
  type?: "anomaly"; ts: number; reading_ts: number | null; device_id: string; metric: Metric;
  value: number; score: number; detector: string;
}
export interface Incident {
  type?: "incident"; incident_id: string; device_id: string; started: number; ended: number;
  anomaly_count: number; metrics: Metric[]; max_score: number;
  samples: [number, Metric, number, number][];
}
export type ProposalStatus = "pending" | "approved" | "rejected" | "executed" | "failed" | "auto_closed";
export interface Proposal {
  proposal_id: string; incident_id: string; device_id: string; diagnosis: string; cause: string;
  action: string; params: Record<string, unknown>; confidence: number; agent: string;
  status: ProposalStatus; ts: number; decided_at: number | null; decided_by: string | null; note: string | null;
}
export interface AuditEntry {
  id: number; ts: number; event: string; actor: string; proposal_id: string | null; details: Record<string, unknown>;
}
export interface DeviceRow {
  device_id: string; last_ts: number; latest: Partial<Record<Metric, number>>; recent_anomalies: number;
  actions?: string[];
}
export interface Health {
  status: string; db: { backend: string; ok: boolean }; uptime_s: number; published: number;
  subscribers: Record<string, { depth: number; dropped: number }>; ws_clients: number; agent: string | null;
  writers: Record<string, { written: number; buffered: number; dropped: number; failures: number }>;
}
export interface Meta { metrics: Record<Metric, { mean: number; std: number }>; devices: string[]; detector: string }
export type LiveEvent = Reading | (Anomaly & { type: "anomaly" }) | (Incident & { type: "incident" })
  | (Proposal & { type: "proposal" })
  | { type: "proposalupdated"; proposal_id: string; device_id: string; status: ProposalStatus; actor: string; ts: number }
  | { type: "actionexecuted"; proposal_id: string; device_id: string; action: string; ts: number };
