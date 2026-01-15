import type { AnyWidget } from "@anywidget/types";

import { createRender } from "@anywidget/react";

import App from "./App";

const widget: AnyWidget = { render: createRender(App) };

export default widget;
