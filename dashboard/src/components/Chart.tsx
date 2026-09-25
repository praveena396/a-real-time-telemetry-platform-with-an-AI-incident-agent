import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import { GridComponent, MarkAreaComponent, TitleComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

// Register only what we use: keeps the bundle far smaller than `import * from "echarts"`.
echarts.use([LineChart, ScatterChart, GridComponent, MarkAreaComponent, TitleComponent, TooltipComponent, CanvasRenderer]);

/** Thin ECharts wrapper: one instance per mount, options merged on each render. */
export function Chart({ option, height = 220 }: { option: echarts.EChartsCoreOption; height?: number }) {
  const el = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  useEffect(() => {
    const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    chart.current = echarts.init(el.current!, dark ? "dark" : undefined, { renderer: "canvas" });
    const ro = new ResizeObserver(() => chart.current?.resize());
    ro.observe(el.current!);
    return () => { ro.disconnect(); chart.current?.dispose(); };
  }, []);
  useEffect(() => { chart.current?.setOption(option, { lazyUpdate: true }); }, [option]);
  return <div ref={el} style={{ width: "100%", height }} />;
}
