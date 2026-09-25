import { useEffect, useState } from "react";
import { api, fmtTime } from "../api";
import type { AuditEntry } from "../types";

export function Audit({ version }: { version: number }) {
  const [rows, setRows] = useState<AuditEntry[]>([]);
  useEffect(() => { api.audit().then(setRows).catch(() => {}); }, [version]);
  return (
    <div className="card">
      <p className="muted">Append-only record of every proposal, validation failure, approval, rejection and execution.</p>
      <table>
        <thead><tr><th>#</th><th>time</th><th>event</th><th>actor</th><th>proposal</th><th>details</th></tr></thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id}>
              <td className="muted">{r.id}</td><td>{fmtTime(r.ts)}</td>
              <td><span className={`pill ${r.event.includes("reject") || r.event.includes("error") || r.event.includes("fail") ? "alert" : r.event.includes("executed") ? "ok" : "muted"}`}>{r.event}</span></td>
              <td>{r.actor}</td><td className="muted">{r.proposal_id ?? ""}</td>
              <td><code className="small">{JSON.stringify(r.details)}</code></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
