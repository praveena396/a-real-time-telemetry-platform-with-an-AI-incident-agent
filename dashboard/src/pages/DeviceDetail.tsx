import { useMemo } from "react";
import { Chart } from "../components/Chart";
import { FaultButtons } from "../components/FaultButtons";
import type { LiveStore } from "../live";
import { key } from "../live";
import { METRICS, UNITS, type Meta, type Metric } from "../types";

function option(store: LiveStore, device: string, m: Metric, meta: Meta | null) {
  const ser = store.series.get(key(device, m)) ?? { ts: [], value: [] };
  const data = ser.ts.map((t, i) => [t * 1000, ser.value[i]]);
  const anomalies = (store.anomalyPoints.get(key(device, m)) ?? []).map(([t, v]) => [t * 1000, v]);
  const base = meta?.metrics[m];
  return {
    animation: false,
    backgroundColor: "transparent",
    title: { text: `${m} (${UNITS[m]})`, textStyle: { fontSize: 13 } },
    grid: { left: 50, right: 16, top: 36, bottom: 28 },
    tooltip: { trigger: "axis" },
    xAxis: { type: "time", splitLine: { show: false } },
    yAxis: { type: "value", scale: true },
    series: [
      {
        type: "line", name: m, data, showSymbol: false, lineStyle: { width: 1.5 },
        markArea: base ? {
          silent: true, itemStyle: { color: "rgba(80,160,120,0.10)" },
          data: [[{ yAxis: base.mean - 3 * base.std }, { yAxis: base.mean + 3 * base.std }]],
        } : undefined,
      },
      { type: "scatter", name: "anomaly", data: anomalies, symbolSize: 9, itemStyle: { color: "#e5484d" } },
    ],
  };
}

export function DeviceDetail({ store, device, meta, tick }: {
  store: LiveStore; device: string; meta: Meta | null; tick: number;
}) {
  const options = useMemo(() => METRICS.map((m) => option(store, device, m, meta)), [tick, device, meta]);
  return (
    <div>
      <div className="toolbar">
        <h2>{device}</h2>
        <FaultButtons device={device} />
      </div>
      <p className="muted">Last 60 s, live. Shaded band = normal range (mean ± 3σ); red dots = detected anomalies.</p>
      <div className="charts">
        {options.map((o, i) => <div className="card" key={METRICS[i]}><Chart option={o} /></div>)}
      </div>
    </div>
  );
}
