import react from "@vitejs/plugin-react";
import path from "path";
import { defineConfig } from "vite";
import { checker } from "vite-plugin-checker";

import { dynamicTSConfig } from "../../vite.config.mts";

export default defineConfig({
  define: {
    "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV ?? "production"),
  },
  plugins: [
    react(),
    checker({
      typescript: {
        tsconfigPath: dynamicTSConfig(),
      },
    }),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "../../out/multimodal-streamlit",
    emptyOutDir: true,
    lib: {
      entry: path.resolve(__dirname, "src/main.tsx"),
      formats: ["es"],
      fileName: () => "assets/index.js",
    },
  },
});
