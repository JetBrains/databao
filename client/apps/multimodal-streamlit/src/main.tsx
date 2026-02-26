import {
  FrontendRenderer,
  FrontendRendererArgs,
} from "@streamlit/component-v2-lib";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import type { AppArgs, MultimodalState } from "./types";

import App from "./App";

import "./styles/main.css";

let root: ReturnType<typeof createRoot> | null = null;

const renderer: FrontendRenderer<MultimodalState, AppArgs> = ({
  data,
  setTriggerValue,
  parentElement,
}: FrontendRendererArgs<MultimodalState, AppArgs>) => {
  if (!root) {
    const rootElement = parentElement.querySelector<HTMLElement>("#root");
    const mountTarget = rootElement ?? parentElement;
    root = createRoot(mountTarget);
  }

  root.render(
    <StrictMode>
      <App data={data} setTriggerValue={setTriggerValue} />
    </StrictMode>,
  );

  return () => {
    root?.unmount();
    root = null;
  };
};

export default renderer;
