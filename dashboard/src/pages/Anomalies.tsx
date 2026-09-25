import { fmtTime } from "../api";
import type { LiveStore } from "../live";

export function Anomalies({ store, open }: { store: LiveStore; open: (d: string) => void }) {
  const rows = store.anomalies.slice(0, 150);
  return (
    <div className="card">
      <table>
        <thead><tr><th>time</th><th>device</th><th>metric</th><th>value</th><th>score</th><th>detector</th><th>latency</th></tr></thead>
        <tbody>
          {rows.length === 0 && <tr><td colSpan={7} className="muted">No anomalies yet. Inject a fault from the Devices page.</td></tr>}
          {rows.map((a, i) => (
            <tr key={`${a.ts}-${i}`}>
              <td>{fmtTime(a.ts)}</td>
              <td><a onClick={() => open(a.device_id)}>{a.device_id}</a></td>
              <td>{a.metric}</td>
              <td>{a.value.toFixed(2)}</td>
              <td className={Math.abs(a.score) > 6 ? "bad" : "warn"}>{a.score.toFixed(1)}</td>
              <td>{a.detector}</td>
              <td className="muted">{a.reading_ts ? `${((a.ts - a.reading_ts) * 1000).toFixed(2)} ms` : "–"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
