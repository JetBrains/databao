import dataclasses
import json
from typing import Any

import pandas as pd
from edaplot.api import make_interactive_spec
from edaplot.llms import LLMConfig as VegaLLMConfig
from edaplot.vega import to_altair_chart
from edaplot.vega_chat.vega_chat import VegaChat, VegaChatConfig

from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, VisualisationResult, Visualizer
from databao.visualizers.vega_vis_tool import VegaVisTool


class VegaChatResult(VisualisationResult):
    spec: dict[str, Any] | None = None
    spec_df: pd.DataFrame | None = None

    def interactive(self, *, force_display: bool = False) -> VegaVisTool | None:
        if self.spec is None or self.spec_df is None:
            return None
        vis_tool = VegaVisTool(self.spec, self.spec_df)
        if force_display:
            vis_tool.display()
        return vis_tool


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
    def __init__(self, llm_config: LLMConfig, *, interactive_charts: bool = False):
        vega_llm = _convert_llm_config(llm_config)
        self._vega_config = VegaChatConfig(
            llm_config=vega_llm,
            data_normalize_column_names=True,  # To deal with column names that have special characters
        )

        # Interactive refers to zooming, panning, etc.
        self._interactive_charts = interactive_charts

    def visualize(self, request: str | None, data: ExecutionResult) -> VegaChatResult:
        if data.df is None:
            return VegaChatResult(text="Nothing to visualize", meta={}, plot=None, code=None)

        if request is None:
            # We could also call the ChartRecommender module, but since we want a
            # single output plot, we'll just use a simple prompt.
            request = (
                "I don't know what the data is about. Show me an interesting plot. Don't show the same plot twice."
            )

        model = VegaChat.from_config(config=self._vega_config, df=data.df)
        model_out = model.query_sync(request)

        spec = model_out.spec
        if spec is None or not model_out.is_drawable or model_out.is_empty_chart:
            return VegaChatResult(
                text=f"Failed to visualize request {request}",
                meta=dataclasses.asdict(model_out),
                plot=None,
                code=None,
            )

        preprocessed_df = model.dataframe
        if self._interactive_charts:
            spec = make_interactive_spec(preprocessed_df, spec)

        text = model_out.message.text()
        spec_json = json.dumps(spec, indent=2)
        # Use the possibly transformed dataframe tied to the generated spec
        altair_chart = to_altair_chart(spec, preprocessed_df)

        return VegaChatResult(
            text=text,
            meta=dataclasses.asdict(model_out),
            plot=altair_chart,
            code=spec_json,
            spec=spec,
            spec_df=preprocessed_df,
        )
