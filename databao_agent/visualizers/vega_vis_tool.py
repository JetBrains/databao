import json
import textwrap
import uuid
from typing import Any

import pandas as pd
from edaplot.data_utils import spec_add_data, spec_remove_data

# Special URL for portus that allows us to deliver fixes without requiring version updates to portus.
# All updates at this link will maintain backward compatibility with the existing usage code.
# If there are changes in the usage API, the version will be incremented (e.g., v1, v2, etc.).
# Versioned links are also available, but using "latest" is suggested
_DATA_TOOLS_URL = "https://resources.jetbrains.com/storage/data-tools/portus"

_VEGA_LITE_SCHEMA_URL = "https://vega.github.io/schema/vega-lite/v5.json"


class VegaVisTool:
    def __init__(
        self, spec: dict[str, Any], df: pd.DataFrame, *, version: str = "v0/latest", debug: bool = True
    ) -> None:
        self._spec = spec
        self._df = df
        self._version = version

        # Using the debug flag will print error information in some cases.
        # You can use the developer tools available in VS Code (Help > Toggle Developer Tools).
        self._debug = debug

    def _repr_html_(self) -> str:
        return self.get_html()

    def get_html(self) -> str:
        spec = self.prepare_spec(self._spec, self._df)

        # Convert to JSON to correctly deal with JS types (e.g., "None" to "null")
        spec_json = json.dumps(spec)
        debug = json.dumps(self._debug)

        div_id = uuid.uuid4()

        # Usage based on https://github.com/JetBrains/data-tools/tree/main/embed
        return textwrap.dedent(f'''
            <div id="{div_id}">
              <script type="application/javascript">
                (function() {{
                  const script = document.createElement("script");
                  script.onload = function() {{
                    const container = document.getElementById("{div_id}");
                    if (container && renderVisualizationTool) {{
                      renderVisualizationTool({spec_json}, container, {debug})
                    }}
                  }};
                  script.src = "{_DATA_TOOLS_URL}/{self._version}/vistool.js";
                  document.getElementById("{div_id}").appendChild(script);
                }})();
              </script>
            </div>
        ''')

    def display(self) -> None:
        from IPython.display import display

        display(self)  # type: ignore[no-untyped-call]

    @classmethod
    def prepare_spec(cls, spec: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        spec = spec.copy()
        if "$schema" not in spec:
            spec["$schema"] = _VEGA_LITE_SCHEMA_URL

        # Remove fields not supported by data-tools that cause no html to be rendered
        spec_remove_data(spec)

        # The data must be included in the spec directly
        spec = spec_add_data(spec, df)
        return spec
