import tseslint from "typescript-eslint";
import baseConfig from "../eslint.config.mjs";

export default tseslint.config(...baseConfig, {
  files: ["src/**/*.{ts,tsx}"],
  languageOptions: {
    parserOptions: {
      project: ["./tsconfig.json"],
      tsconfigRootDir: import.meta.dirname,
    },
  },
});
