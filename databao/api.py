from typing import TextIO

from databao.caches.in_mem_cache import InMemCache
from databao.configs.agent import DEFAULT_AGENT_CONFIG, AgentConfig
from databao.configs.llm import LLMConfig, LLMConfigDirectory
from databao.core import Agent, AgentV1, Cache, Executor, Visualizer
from databao.core.v2.agent import AgentV2
from databao.core.v2.context import Context
from databao.dbt.config import DbtConfig
from databao.executors.lighthouse.executor import LighthouseExecutor
from databao.executors.react_duckdb.executor import ReactDuckDBExecutor
from databao.visualizers.vega_chat import VegaChatVisualizer


def new_agent(
    name: str | None = None,
    llm_config: LLMConfig | None = None,
    agent_config: AgentConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    rows_limit: int = 1000,
    stream_ask: bool = True,
    stream_plot: bool = False,
    lazy_threads: bool = False,
    auto_output_modality: bool = True,
    writer: TextIO | None = None,
    executor_type: str = "lighthouse",
    dbt_config: DbtConfig | None = None,
) -> Agent:
    """This is an entry point for users to create a new agent.
    Agent can't be modified after it's created. Only new data sources can be added.

    Args:
        name: Agent name (default: "default_agent")
        llm_config: LLM configuration
        agent_config: Agent configuration
        data_executor: Custom executor (overrides executor_type if provided)
        visualizer: Custom visualizer
        cache: Custom cache
        rows_limit: Max rows to materialize
        stream_ask: Enable streaming for ask()
        stream_plot: Enable streaming for plot()
        lazy_threads: Enable lazy thread evaluation
        auto_output_modality: Auto-detect output modality
        writer: TextIO for streaming output (e.g., for Streamlit integration)
        executor_type: Executor type ("lighthouse" or "react_duckdb")

    Returns:
        Configured Agent instance
    """
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    agent_config = agent_config if agent_config else DEFAULT_AGENT_CONFIG

    # Create executor if not provided
    if data_executor is None:
        match executor_type:
            case "react_duckdb":
                data_executor = ReactDuckDBExecutor(writer=writer)
            case "lighthouse":
                data_executor = LighthouseExecutor(writer=writer)
            case _:
                raise ValueError(f"Invalid executor type: {executor_type}")

    return AgentV1(
        llm_config,
        agent_config,
        name=name or "default_agent",
        data_executor=data_executor,
        visualizer=visualizer or VegaChatVisualizer(llm_config),
        cache=cache or InMemCache(),
        rows_limit=rows_limit,
        stream_ask=stream_ask,
        stream_plot=stream_plot,
        lazy_threads=lazy_threads,
        auto_output_modality=auto_output_modality,
    )


def new_agent_v2(
    context: Context,
    name: str | None = None,
    llm_config: LLMConfig | None = None,
    agent_config: AgentConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    rows_limit: int = 1000,
    stream_ask: bool = True,
    stream_plot: bool = False,
    lazy_threads: bool = False,
    auto_output_modality: bool = True,
    executor_type: str = "lighthouse",
    writer: TextIO | None = None,
    dbt_config: DbtConfig | None = None,
) -> AgentV2:
    """This is an entry point for users to create a new agent.
    Agent can't be modified after it's created. Only new data sources can be added.

    Args:
        context: Context object containing DCE engine and sources
        name: Agent name (default: "default_agent")
        llm_config: LLM configuration
        agent_config: Agent configuration
        data_executor: Custom executor (overrides executor_type if provided)
        visualizer: Custom visualizer
        cache: Custom cache
        rows_limit: Max rows to materialize
        stream_ask: Enable streaming for ask()
        stream_plot: Enable streaming for plot()
        lazy_threads: Enable lazy thread evaluation
        auto_output_modality: Auto-detect output modality
        executor_type: Executor type ("lighthouse" or "react_duckdb")
        writer: TextIO for streaming output (e.g., for Streamlit integration)

    Returns:
        Configured AgentV2 instance
    """
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    agent_config = agent_config if agent_config else DEFAULT_AGENT_CONFIG

    if data_executor is None:
        match executor_type:
            case "react_duckdb":
                data_executor = ReactDuckDBExecutor(writer=writer)
            case "lighthouse":
                data_executor = LighthouseExecutor(writer=writer)
            case _:
                raise ValueError(f"Invalid executor type: {executor_type}")

    return AgentV2(
        context,
        llm_config,
        agent_config,
        name=name or "default_agent",
        data_executor=data_executor,
        visualizer=visualizer or VegaChatVisualizer(llm_config),
        cache=cache or InMemCache(),
        rows_limit=rows_limit,
        stream_ask=stream_ask,
        stream_plot=stream_plot,
        lazy_threads=lazy_threads,
        auto_output_modality=auto_output_modality,
        dbt_config=dbt_config,
    )
