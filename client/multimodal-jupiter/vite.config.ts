import react from "@vitejs/plugin-react";
import path from "path";
import { defineConfig } from "vite";
import { checker } from "vite-plugin-checker";

import { dynamicTSConfig } from "../vite.config.mts";

export default defineConfig({
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
  define: {
    "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV),
  },
  build: {
    outDir: "../out/multimodal-jupiter",
    emptyOutDir: true,
    lib: {
      entry: path.resolve(__dirname, "src/main.tsx"),
      formats: ["es"],
      fileName: () => "index.js",
    },
    cssCodeSplit: false,
    rollupOptions: {
      external: [],
      output: {
        assetFileNames: "style.css",
      },
    },
  },
});
