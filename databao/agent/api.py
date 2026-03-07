from pathlib import Path
from typing import TextIO

from typing_extensions import deprecated

from databao.agent.caches.in_mem_cache import InMemCache
from databao.agent.configs.agent import DEFAULT_AGENT_CONFIG, AgentConfig
from databao.agent.configs.llm import LLMConfig, LLMConfigDirectory
from databao.agent.core import Agent, Cache, Executor, Visualizer
from databao.agent.core.domain import Domain, _DCEProjectDomain, _InMemoryDomain
from databao.agent.executors import ReactDuckDBExecutor
from databao.agent.executors.dbt.config import DbtConfig
from databao.agent.executors.dbt.executor import DbtProjectExecutor
from databao.agent.executors.lighthouse.executor import LighthouseExecutor
from databao.agent.visualizers.vega_chat import VegaChatVisualizer


def agent(
    domain: Domain,
    *,
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
    """
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    agent_config = agent_config if agent_config else DEFAULT_AGENT_CONFIG

    if data_executor is None:
        match executor_type:
            case "lighthouse":
                data_executor = LighthouseExecutor(writer=writer)
            case "dbt":
                if dbt_config is None:
                    dbt_config = DbtConfig()
                data_executor = DbtProjectExecutor(dbt_config=dbt_config, writer=writer)
            case "react_duckdb":
                data_executor = ReactDuckDBExecutor(writer=writer)
            case _:
                raise ValueError(f"Invalid executor type: {executor_type}")

    return Agent(
        domain,
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


def domain(project_dir: str | Path | None = None) -> Domain:
    if isinstance(project_dir, str):
        project_dir = Path(project_dir)
    if project_dir is None:
        return _InMemoryDomain()
    else:
        return _DCEProjectDomain(project_dir)


@deprecated("Use agent() instead.")
def new_agent(
    name: str | None = None,
    llm_config: LLMConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    rows_limit: int = 1000,
    stream_ask: bool = True,
    stream_plot: bool = False,
    lazy_threads: bool = False,
    auto_output_modality: bool = True,
) -> Agent:
    raise NotImplementedError("This method was removed. Use agent() instead.")
