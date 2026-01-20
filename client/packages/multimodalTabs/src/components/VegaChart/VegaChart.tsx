import { useEffect, useRef } from "react";
import embed from "vega-embed";

interface VegaChartProps {
  spec: object;
}

export function VegaChart({ spec }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const embedChart = async () => {
      if (!containerRef.current || !spec) return;

      try {
        await embed(containerRef.current, spec, {
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
      if (containerRef.current) {
        containerRef.current.innerHTML = "";
      }
    };
  }, [spec]);

  return <div ref={containerRef} />;
}
