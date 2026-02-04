import { readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { resolve } from "node:path";

type PackageJson = {
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
};

const nodeBuiltins = new Set([
  ...builtinModules,
  ...builtinModules.map((m) => `node:${m}`),
]);

export const externalizeAllDependencies = (id: string, path: string) => {
  const parsedPackageJson = JSON.parse(
    readFileSync(path, "utf8"),
  ) as PackageJson;

  const depNames = new Set([
    ...Object.keys(parsedPackageJson.dependencies ?? {}),
    ...Object.keys(parsedPackageJson.peerDependencies ?? {}),
    ...Object.keys(parsedPackageJson.optionalDependencies ?? {}),
  ]);

  return (
    nodeBuiltins.has(id) ||
    [...depNames].some((dep) => id === dep || id.startsWith(dep + "/"))
  );
};

export const dynamicTSConfig = () => {
  const isDev = process.env.NODE_ENV == "development";
  return resolve(process.cwd(), `./tsconfig${isDev ? ".dev" : ""}.json`);
};
