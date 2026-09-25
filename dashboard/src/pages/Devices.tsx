import type { LiveStore } from "../live";
import { key } from "../live";
import { METRICS, UNITS, type Meta } from "../types";
import { FaultButtons } from "../components/FaultButtons";

export function Devices({ store, meta, pending, open }: {
  store: LiveStore; meta: Meta | null; pending: Set<string>; open: (d: string) => void;
}) {
  const now = Date.now() / 1000;
  const devices = meta?.devices ?? [];
  return (
    <div className="grid">
      {devices.map((d) => {
        const last = store.lastAnomaly.get(d);
        const hot = last !== undefined && now - last < 15;
        const status = pending.has(d) ? "pending" : hot ? "alert" : "ok";
        return (
          <div key={d} className={`card device ${status}`} onClick={() => open(d)}>
            <div className="device-head">
              <strong>{d}</strong>
              <span className={`pill ${status}`}>
                {status === "pending" ? "awaiting approval" : status === "alert" ? "anomaly" : "normal"}
              </span>
            </div>
            {METRICS.map((m) => {
              const v = store.latest.get(key(d, m));
              const base = meta?.metrics[m];
              const z = v !== undefined && base ? Math.abs(v - base.mean) / base.std : 0;
              return (
                <div key={m} className="metric-row">
                  <span className="muted">{m}</span>
                  <span className={z > 4 ? "bad" : z > 2.5 ? "warn" : ""}>
                    {v === undefined ? "–" : v.toFixed(2)} <small className="muted">{UNITS[m]}</small>
                  </span>
                </div>
              );
            })}
            <div onClick={(e) => e.stopPropagation()}><FaultButtons device={d} compact /></div>
          </div>
        );
      })}
    </div>
  );
}
