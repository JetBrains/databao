import type { AnyWidget } from "@anywidget/types";

import { createRender } from "@anywidget/react";

import App from "./App";

import "./styles/main.css";

const widget: AnyWidget = { render: createRender(App) };

export default widget;
