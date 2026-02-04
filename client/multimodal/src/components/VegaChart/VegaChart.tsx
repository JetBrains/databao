import { useEffect, useRef } from "react";
import embed from "vega-embed";

interface VegaChartProps {
  spec: object;
}

export function VegaChart({ spec }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const containerEl = containerRef.current;

    const embedChart = async () => {
      if (!containerEl || !spec) return;

      try {
        await embed(containerEl, spec, {
          actions: {
            export: true,
            source: false,
            compiled: false,
            editor: false,
          },
          renderer: "svg",
        });
      } catch (error) {
        console.error("Failed to render Vega chart:", error);
      }
    };

    embedChart();

    return () => {
      if (containerEl) {
        containerEl.innerHTML = "";
      }
    };
  }, [spec]);

  return <div ref={containerRef} />;
}
