import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "./api";
import { useLive } from "./live";
import { Anomalies } from "./pages/Anomalies";
import { Approvals } from "./pages/Approvals";
import { Audit } from "./pages/Audit";
import { DeviceDetail } from "./pages/DeviceDetail";
import { Devices } from "./pages/Devices";
import type { Health, Incident, LiveEvent, Meta, Proposal } from "./types";

type Route = { page: "devices" | "device" | "anomalies" | "approvals" | "audit"; device?: string };

function parseHash(): Route {
  const [, page, device] = location.hash.replace(/^#/, "").split("/");
  if (page === "device" && device) return { page: "device", device };
  if (page === "anomalies" || page === "approvals" || page === "audit") return { page };
  return { page: "devices" };
}

export default function App() {
  const [route, setRoute] = useState<Route>(parseHash);
  const [meta, setMeta] = useState<Meta | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [proposals, setProposals] = useState<Proposal[]>([]);
  const [incidents, setIncidents] = useState<Map<string, Incident>>(new Map());
  const [version, setVersion] = useState(0);

  const refresh = useCallback(() => {
    api.proposals().then(setProposals).catch(() => {});
    api.incidents().then((xs) => setIncidents(new Map(xs.map((i) => [i.incident_id, i])))).catch(() => {});
    setVersion((v) => v + 1);
  }, []);

  // Control-plane events (incidents, proposals, decisions) trigger a refetch.
  const onControl = useCallback((e: LiveEvent) => {
    if (e.type === "incident" || e.type === "proposal" || e.type === "proposalupdated" || e.type === "actionexecuted") refresh();
  }, [refresh]);
  const { store, tick, connected } = useLive(onControl);

  useEffect(() => {
    const onHash = () => setRoute(parseHash());
    window.addEventListener("hashchange", onHash);
    api.meta().then(setMeta).catch(() => {});
    refresh();
    const h = () => api.health().then(setHealth).catch(() => setHealth(null));
    h();
    const t = window.setInterval(h, 2000);
    return () => { window.removeEventListener("hashchange", onHash); window.clearInterval(t); };
  }, [refresh]);

  const pendingDevices = useMemo(
    () => new Set(proposals.filter((p) => p.status === "pending").map((p) => p.device_id)), [proposals]);
  const pendingCount = pendingDevices.size;
  const open = (d: string) => { location.hash = `#/device/${d}`; };
  const drops = health ? Object.values(health.subscribers).reduce((a, s) => a + s.dropped, 0) : 0;

  return (
    <div className="app">
      <header>
        <h1>Telemetry Sentinel</h1>
        <nav>
          <a className={route.page === "devices" || route.page === "device" ? "active" : ""} href="#/devices">Devices</a>
          <a className={route.page === "anomalies" ? "active" : ""} href="#/anomalies">Anomalies</a>
          <a className={route.page === "approvals" ? "active" : ""} href="#/approvals">
            Approvals {pendingCount > 0 && <span className="pill pending">{pendingCount}</span>}
          </a>
          <a className={route.page === "audit" ? "active" : ""} href="#/audit">Audit log</a>
        </nav>
        <div className="status">
          <span className={`dot ${connected ? "ok" : "bad"}`} /> {connected ? "live" : "reconnecting"}
          {health && (
            <>
              <span>db: <b className={health.db.ok ? "" : "bad"}>{health.db.backend}{health.db.ok ? "" : " (down)"}</b></span>
              <span>events: <b>{health.published.toLocaleString()}</b></span>
              <span>drops: <b className={drops ? "bad" : ""}>{drops}</b></span>
              <span>agent: <b>{health.agent ?? "off"}</b></span>
            </>
          )}
        </div>
      </header>
      <main>
        {route.page === "devices" && <Devices store={store} meta={meta} pending={pendingDevices} open={open} />}
        {route.page === "device" && route.device && <DeviceDetail store={store} device={route.device} meta={meta} tick={tick} />}
        {route.page === "anomalies" && <Anomalies store={store} open={open} />}
        {route.page === "approvals" && <Approvals proposals={proposals} incidents={incidents} refresh={refresh} open={open} />}
        {route.page === "audit" && <Audit version={version} />}
      </main>
    </div>
  );
}
