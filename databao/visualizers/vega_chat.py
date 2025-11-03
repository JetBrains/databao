import dataclasses
import json

from edaplot.llms import LLMConfig as VegaLLMConfig
from edaplot.vega import to_altair_chart
from edaplot.vega_chat.vega_chat import VegaChat, VegaChatConfig

from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, VisualisationResult, Visualizer


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
    def __init__(self, llm_config: LLMConfig):
        vega_llm = _convert_llm_config(llm_config)
        self._vega_config = VegaChatConfig(
            llm_config=vega_llm,
            data_normalize_column_names=True,  # To deal with column names that have special characters
        )

    def visualize(self, request: str | None, data: ExecutionResult) -> VisualisationResult:
        if data.df is None:
            return VisualisationResult(text="Nothing to visualize", meta={}, plot=None, code=None)

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
            return VisualisationResult(
                text=f"Failed to visualize request {request}", meta=dataclasses.asdict(model_out), plot=None, code=None
            )

        text = model_out.message.text()
        spec_json = json.dumps(spec, indent=2)
        # Use the possibly transformed dataframe tied to the generated spec
        altair_chart = to_altair_chart(spec, model.dataframe)

        return VisualisationResult(text=text, meta=dataclasses.asdict(model_out), plot=altair_chart, code=spec_json)
