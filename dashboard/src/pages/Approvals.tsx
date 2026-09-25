import { useState } from "react";
import { ago, api, fmtTime } from "../api";
import type { Incident, Proposal } from "../types";

function readActor(): string {
  try { return localStorage.getItem("sentinel.actor") ?? ""; } catch { return ""; }
}

export function Approvals({ proposals, incidents, refresh, open }: {
  proposals: Proposal[]; incidents: Map<string, Incident>; refresh: () => void; open: (d: string) => void;
}) {
  const [actor, setActor] = useState(readActor);
  const [notes, setNotes] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<string | null>(null);

  const saveActor = (v: string) => {
    setActor(v);
    try { localStorage.setItem("sentinel.actor", v); } catch { /* storage unavailable */ }
  };
  const decide = async (p: Proposal, d: "approve" | "reject") => {
    if (!actor.trim()) { setError("Enter your name first: every decision is recorded in the audit log."); return; }
    setBusy(p.proposal_id);
    setError("");
    try { await api.decide(p.proposal_id, d, actor.trim(), notes[p.proposal_id]); }
    catch (e) { setError(String(e)); }
    finally { setBusy(null); refresh(); }
  };

  const pending = proposals.filter((p) => p.status === "pending");
  const decided = proposals.filter((p) => p.status !== "pending" && p.status !== "auto_closed").slice(0, 30);
  const auto = proposals.filter((p) => p.status === "auto_closed").slice(0, 30);

  return (
    <div>
      <div className="toolbar">
        <h2>Approval queue <span className="pill pending">{pending.length}</span></h2>
        <label>Reviewer&nbsp;<input value={actor} onChange={(e) => saveActor(e.target.value)} placeholder="your name" /></label>
      </div>
      <p className="muted">
        The agent can only propose. Nothing touches a device until someone approves it here.
        Proposals that change nothing (no_action, increase_monitoring) are closed by policy and listed below.
      </p>
      {error && <div className="card bad">{error}</div>}
      {pending.length === 0 && <div className="card muted">No proposals waiting. Inject a drift or stuck fault to trigger the agent.</div>}
      {pending.map((p) => {
        const inc = incidents.get(p.incident_id);
        return (
          <div className="card proposal" key={p.proposal_id}>
            <div className="device-head">
              <div>
                <strong>{p.action}</strong>{Object.keys(p.params).length > 0 && <code> {JSON.stringify(p.params)}</code>}
                {" on "}<a onClick={() => open(p.device_id)}>{p.device_id}</a>
              </div>
              <span className="muted">{ago(p.ts)} · {p.agent}</span>
            </div>
            <p><span className="pill alert">{p.diagnosis}</span> {p.cause}</p>
            <div className="confidence"><div style={{ width: `${p.confidence * 100}%` }} /></div>
            <small className="muted">confidence {(p.confidence * 100).toFixed(0)}%</small>
            {inc && (
              <p className="muted small">
                Incident {inc.incident_id}: {inc.anomaly_count} anomalies on {inc.metrics.join(", ")} between{" "}
                {fmtTime(inc.started)} and {fmtTime(inc.ended)}, peak score {inc.max_score.toFixed(1)}
              </p>
            )}
            <div className="actions">
              <input placeholder="note (optional)" value={notes[p.proposal_id] ?? ""}
                onChange={(e) => setNotes({ ...notes, [p.proposal_id]: e.target.value })} />
              <button disabled={busy === p.proposal_id} onClick={() => decide(p, "approve")}>Approve</button>
              <button disabled={busy === p.proposal_id} className="danger" onClick={() => decide(p, "reject")}>Reject</button>
            </div>
          </div>
        );
      })}
      <h3>Recent decisions</h3>
      <div className="card">
        <table>
          <thead><tr><th>decided</th><th>device</th><th>diagnosis</th><th>action</th><th>status</th><th>by</th><th>note</th></tr></thead>
          <tbody>
            {decided.map((p) => (
              <tr key={p.proposal_id}>
                <td>{p.decided_at ? fmtTime(p.decided_at) : "–"}</td>
                <td>{p.device_id}</td><td>{p.diagnosis}</td><td>{p.action}</td>
                <td><span className={`pill ${p.status === "executed" ? "ok" : p.status === "rejected" ? "muted" : "alert"}`}>{p.status}</span></td>
                <td>{p.decided_by}</td><td className="muted">{p.note}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <h3>Auto-closed (no-op) diagnoses</h3>
      <div className="card">
        <table>
          <thead><tr><th>time</th><th>device</th><th>diagnosis</th><th>action</th><th>cause</th></tr></thead>
          <tbody>
            {auto.map((p) => (
              <tr key={p.proposal_id}>
                <td>{fmtTime(p.ts)}</td><td>{p.device_id}</td><td>{p.diagnosis}</td><td>{p.action}</td>
                <td className="muted">{p.cause}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
