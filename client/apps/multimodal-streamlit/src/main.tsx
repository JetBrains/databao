import {
  FrontendRenderer,
  FrontendRendererArgs,
} from "@streamlit/component-v2-lib";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import type { AppArgs, MultimodalState } from "./types";

import App from "./App";

import "./styles/main.css";

const renderer: FrontendRenderer<MultimodalState, AppArgs> = ({
  data,
  setTriggerValue,
  parentElement,
}: FrontendRendererArgs<MultimodalState, AppArgs>) => {
  const mountTarget = parentElement.querySelector<HTMLElement>("#root") ?? parentElement;
  const root = createRoot(mountTarget);
  root.render(
    <StrictMode>
      <App
        data={data}
        setTriggerValue={setTriggerValue}
      />
    </StrictMode>,
  );
  return () => root.unmount();
};

export default renderer;
