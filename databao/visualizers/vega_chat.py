import dataclasses
import io
import json
import logging
from typing import Any

import altair
import pandas as pd
from edaplot.image_utils import vl_to_png_bytes
from edaplot.llms import LLMConfig as VegaLLMConfig
from edaplot.vega import to_altair_chart
from edaplot.vega_chat.vega_chat import VegaChat, VegaChatConfig, VegaChatState
from langchain_core.runnables import RunnableConfig
from PIL import Image

from databao.agents.base import AgentExecutor
from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, VisualisationResult, Visualizer
from databao.visualizers.vega_vis_tool import VegaVisTool

logger = logging.getLogger(__name__)


class VegaChatResult(VisualisationResult):
    plot: VegaVisTool | altair.Chart | Image.Image | None = None
    spec: dict[str, Any] | None = None
    spec_df: pd.DataFrame | None = None

    def interactive(self) -> VegaVisTool | None:
        """Return an interactive UI wizard for the Vega-Lite chart.

        The returned chart object can be rendered in interactive notebooks."""
        if self.spec is None or self.spec_df is None:
            return None
        return VegaVisTool(self.spec, self.spec_df)

    def altair(self) -> altair.Chart | None:
        """Return an interactive Altair chart.

        The returned chart object can be rendered in interactive notebooks."""
        if self.spec is None or self.spec_df is None:
            return None
        return to_altair_chart(self.spec, self.spec_df)

    def image(self) -> Image.Image | None:
        """Return a static PIL.Image.Image."""
        if self.spec is None or self.spec_df is None:
            return None
        if (png_bytes := vl_to_png_bytes(self.spec, self.spec_df)) is not None:
            return Image.open(io.BytesIO(png_bytes))
        return None


def _convert_llm_config(llm_config: LLMConfig) -> VegaLLMConfig:
    # N.B. The two config classes are nearly identical.
    return VegaLLMConfig(
        name=llm_config.name,
        temperature=llm_config.temperature,
        max_tokens=llm_config.max_tokens,
        reasoning_effort=llm_config.reasoning_effort,
        cache_system_prompt=llm_config.cache_system_prompt,
        timeout=llm_config.timeout,
        api_base_url=llm_config.api_base_url,
        use_responses_api=llm_config.use_responses_api,
        ollama_pull_model=llm_config.ollama_pull_model,
        model_kwargs=llm_config.model_kwargs,
    )


class VegaChatVisualizer(Visualizer):
    def __init__(self, llm_config: LLMConfig, *, return_interactive_chart: bool = False):
        vega_llm = _convert_llm_config(llm_config)
        self._vega_config = VegaChatConfig(
            llm_config=vega_llm,
            data_normalize_column_names=True,  # To deal with column names that have special characters
        )
        self._return_interactive_chart = return_interactive_chart

    def visualize(self, request: str | None, data: ExecutionResult, *, stream: bool = True) -> VegaChatResult:
        if data.df is None:
            return VegaChatResult(text="Nothing to visualize", meta={}, plot=None, code=None)

        if request is None:
            # We could also call the ChartRecommender module, but since we want a
            # single output plot, we'll just use a simple prompt.
            request = (
                "I don't know what the data is about. Show me an interesting plot. Don't show the same plot twice."
            )

        vega_chat = VegaChat.from_config(config=self._vega_config, df=data.df)
        start_state, compiled_graph = vega_chat.start_query(request, is_async=False)
        # Use an empty `config` instead of `None` due to a bug in the "AI Agents Debugger" PyCharm plugin.
        final_state: VegaChatState = AgentExecutor._invoke_graph_sync(
            compiled_graph, start_state, config=RunnableConfig(), stream=stream
        )
        model_out = vega_chat.submit_query(final_state)

        # Use the possibly transformed dataframe tied to the generated spec
        preprocessed_df = vega_chat.dataframe
        text = model_out.message.text()
        meta = dataclasses.asdict(model_out)
        spec = model_out.spec
        spec_json = json.dumps(spec, indent=2) if spec is not None else None
        if spec is None or not model_out.is_drawable or model_out.is_empty_chart:
            return VegaChatResult(
                text=f"Failed to visualize request! Output: {text}",
                meta=meta,
                plot=None,
                code=spec_json,
                spec=spec,
                spec_df=preprocessed_df,
            )

        if not model_out.is_valid_schema and model_out.is_drawable:
            # Vega-Lite specs can be invalid (so cannot be used with altair), but they might still be drawable with
            # another backend.
            logger.warning("Generated Vega-Lite spec is not valid, but it is still drawable: %s", spec_json)
            if self._return_interactive_chart:
                # The VegaVisTool backend uses vega-embed so it can handle corrupt specs
                plot = VegaVisTool(spec, preprocessed_df)
            elif (png_bytes := vl_to_png_bytes(spec, preprocessed_df)) is not None:
                # Try to convert to an Image that can still be displayed in Jupyter notebooks
                plot = Image.open(io.BytesIO(png_bytes))
            else:
                return VegaChatResult(
                    text=f"Failed to visualize request! Output: {text}",
                    meta=meta,
                    plot=None,
                    code=spec_json,
                    spec=spec,
                    spec_df=preprocessed_df,
                )
        elif self._return_interactive_chart:
            plot = VegaVisTool(spec, preprocessed_df)
        else:
            plot = to_altair_chart(spec, preprocessed_df)

        return VegaChatResult(
            text=text,
            meta=meta,
            plot=plot,
            code=spec_json,
            spec=spec,
            spec_df=preprocessed_df,
        )
