import asyncio
import json

from edaplot.api import generate_query_chart
from edaplot.llms import LLMConfig as VegaLLMConfig
from edaplot.vega import to_altair_chart
from edaplot.vega_chat.vega_chat import VegaChatConfig

from portus.configs.llm import LLMConfig
from portus.core import ExecutionResult, VisualisationResult, Visualizer


def _convert_llm_config(llm_config: LLMConfig) -> VegaLLMConfig:
    # The two config classes are identical
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
    def __init__(self, llm_config: LLMConfig):
        vega_llm = _convert_llm_config(llm_config)
        self._vega_config = VegaChatConfig(
            llm_config=vega_llm,
            data_normalize_column_names=True,  # To deal with column names that have special characters
        )

    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        if data.df is None:
            return VisualisationResult(text="Nothing to visualize", meta={}, plot=None, code=None)

        coro = generate_query_chart(request, data.df, self._vega_config, make_interactive=False)
        chart_result = asyncio.run(coro)
        if chart_result is None:
            return VisualisationResult(text=f"Failed to visualize request {request}", meta={}, plot=None, code=None)

        spec_json = json.dumps(chart_result.spec, indent=2)
        altair_chart = to_altair_chart(chart_result.spec, chart_result.dataframe)
        return VisualisationResult(text="", meta=dict(chart_result=chart_result), plot=altair_chart, code=spec_json)
