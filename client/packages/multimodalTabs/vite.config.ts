import { resolve } from "node:path";
import path from "path";
import { defineConfig } from "vite";
import checker from "vite-plugin-checker";
import dts from "vite-plugin-dts";
import { libInjectCss } from "vite-plugin-lib-inject-css";

import {
  dynamicTSConfig,
  externalizeAllDependencies,
} from "../../vite.config.mjs";

export default defineConfig({
  plugins: [
    dts({
      include: ["src"],
      outDir: "dist",
    }),
    libInjectCss(),
    dts({
      rollupTypes: false,
      tsconfigPath: dynamicTSConfig(),
    }),
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
    sourcemap: true,
    lib: {
      entry: {
        index: path.resolve(__dirname, "src/index.ts"),
      },
      formats: ["es"],
    },
    rollupOptions: {
      external: (id) =>
        externalizeAllDependencies(
          id,
          resolve(process.cwd(), "./package.json"),
        ),
      output: {
        preserveModules: true,
        preserveModulesRoot: "src",
        entryFileNames: "[name].js",
        assetFileNames: (assetInfo) => {
          if (assetInfo.names[0]?.endsWith(".module.css")) {
            return "[name]-[hash][extname]";
          }
          return "[name].[ext]";
        },
        sourcemapExcludeSources: false,
      },
    },
    outDir: "dist",
    emptyOutDir: true,
  },
});
