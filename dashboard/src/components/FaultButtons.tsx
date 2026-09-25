import { useState } from "react";
import { api } from "../api";
import { METRICS } from "../types";

/** Demo controls: inject a fault into a simulated device and watch it flow through the system. */
export function FaultButtons({ device, compact = false }: { device: string; compact?: boolean }) {
  const [metric, setMetric] = useState<string>("temperature");
  const [msg, setMsg] = useState("");
  const inject = async (kind: string) => {
    try {
      await api.inject(device, metric, kind);
      setMsg(`${kind} injected`);
    } catch (e) {
      setMsg(String(e));
    }
    setTimeout(() => setMsg(""), 2000);
  };
  return (
    <div className={`faults ${compact ? "compact" : ""}`}>
      <select value={metric} onChange={(e) => setMetric(e.target.value)} aria-label="metric">
        {METRICS.map((m) => <option key={m}>{m}</option>)}
      </select>
      {["spike", "drift", "stuck"].map((k) => (
        <button key={k} className="ghost" onClick={() => inject(k)}>{k}</button>
      ))}
      {msg && <small className="muted">{msg}</small>}
    </div>
  );
}
