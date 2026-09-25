import { useEffect, useRef, useState } from "react";
import type { Anomaly, LiveEvent, Metric } from "./types";

const WINDOW_S = 60;

export interface LiveStore {
  series: Map<string, { ts: number[]; value: number[] }>; // key: device|metric
  latest: Map<string, number>;                             // key: device|metric
  lastAnomaly: Map<string, number>;                        // device -> ts
  anomalies: Anomaly[];                                    // newest first, capped
  anomalyPoints: Map<string, [number, number][]>;          // device|metric -> [ts, value]
  received: number;
  dropped: number;
  lastEventAt: number;
}

const newStore = (): LiveStore => ({
  series: new Map(), latest: new Map(), lastAnomaly: new Map(), anomalies: [], anomalyPoints: new Map(),
  received: 0, dropped: 0, lastEventAt: 0,
});

export const key = (device: string, metric: Metric | string) => `${device}|${metric}`;

/**
 * One WebSocket for the whole app. Events land in a mutable store (no
 * re-render per event); components re-render on a 250 ms tick. At ~600
 * events/s this keeps React work constant regardless of event rate.
 */
export function useLive(onControl: (e: LiveEvent) => void) {
  const store = useRef<LiveStore>(newStore());
  const [tick, setTick] = useState(0);
  const [connected, setConnected] = useState(false);
  const control = useRef(onControl);
  control.current = onControl;

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry: number | undefined;
    let closed = false;
    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/ws`);
      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!closed) retry = window.setTimeout(connect, 1500);
      };
      ws.onmessage = (msg) => {
        const { events, dropped } = JSON.parse(msg.data) as { events: LiveEvent[]; dropped: number };
        const s = store.current;
        s.dropped = dropped;
        s.received += events.length;
        s.lastEventAt = Date.now() / 1000;
        const cutoff = Date.now() / 1000 - WINDOW_S;
        for (const e of events) {
          if (e.type === "reading") {
            const k = key(e.device_id, e.metric);
            let ser = s.series.get(k);
            if (!ser) s.series.set(k, (ser = { ts: [], value: [] }));
            ser.ts.push(e.ts);
            ser.value.push(e.value);
            let drop = 0;
            while (drop < ser.ts.length && ser.ts[drop] < cutoff) drop++;
            if (drop) { ser.ts.splice(0, drop); ser.value.splice(0, drop); }
            s.latest.set(k, e.value);
          } else if (e.type === "anomaly") {
            s.anomalies.unshift(e);
            if (s.anomalies.length > 300) s.anomalies.length = 300;
            s.lastAnomaly.set(e.device_id, e.ts);
            const k = key(e.device_id, e.metric);
            const pts = s.anomalyPoints.get(k) ?? [];
            pts.push([e.reading_ts ?? e.ts, e.value]);
            s.anomalyPoints.set(k, pts.filter((p) => p[0] >= cutoff));
          } else {
            control.current(e);
          }
        }
      };
    };
    connect();
    const t = window.setInterval(() => setTick((x) => x + 1), 250);
    return () => { closed = true; window.clearTimeout(retry); window.clearInterval(t); ws?.close(); };
  }, []);

  return { store: store.current, tick, connected };
}
