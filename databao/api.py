from pathlib import Path
from typing import TextIO, cast

from databao.caches.in_mem_cache import InMemCache
from databao.configs.agent import DEFAULT_AGENT_CONFIG, AgentConfig
from databao.configs.llm import LLMConfig, LLMConfigDirectory
from databao.core import Agent, Cache, Executor, Visualizer
from databao.core.domain import Domain, _Domain, _InMemoryDomain, _PersistentDomain
from databao.executors.dbt.config import DbtConfig
from databao.executors.dbt.executor import DbtProjectExecutor
from databao.executors.lighthouse.executor import LighthouseExecutor
from databao.visualizers.vega_chat import VegaChatVisualizer


def agent(
    domain: Domain,
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
    domain = cast(_Domain, domain)
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    agent_config = agent_config if agent_config else DEFAULT_AGENT_CONFIG

    # Create executor if not provided
    if data_executor is None:
        match executor_type:
            case "lighthouse":
                data_executor = LighthouseExecutor(writer=writer)
            case "dbt":
                if dbt_config is None:
                    raise ValueError("dbt_config must be provided when executor_type='dbt'")
                data_executor = DbtProjectExecutor(dbt_config=dbt_config, writer=writer)
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
        return _PersistentDomain(project_dir)
