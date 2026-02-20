import logging
from pathlib import Path

import click

import databao
from databao import LLMConfig
from databao.api import domain
from databao.configs.agent import AgentConfig
from databao.executors.dbt import DbtProjectExecutor
from databao.executors.query_expansion import QueryExpansionConfig

logging.basicConfig(level=logging.INFO)

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent_config = AgentConfig(recursion_limit=100, parallel_tool_calls=True)


@click.command()
@click.argument("dce_project", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--expand/--no-expand", default=False, help="Enable LLM query expansion with RRF re-ranking.")
def main(dce_project: Path, expand: bool) -> None:
    domain_ctx = domain(project_dir=dce_project)

    expansion_config = QueryExpansionConfig(num_queries=3, rrf_k=60) if expand else None

    agent = databao.agent(
        domain=domain_ctx,
        name="demo-dbt-executor",
        llm_config=llm_config,
        agent_config=agent_config,
        data_executor=DbtProjectExecutor(expansion_config=expansion_config),
    )

    thread = agent.thread(stream_ask=True)

    thread.ask("What is our refund rate by month?")

    print("\n=== TEXT ===\n")
    print(thread.text())

    print("\n=== CODE ===\n")
    print(thread.code())

    print("\n=== Dataframe ===\n")
    print(thread.df())


if __name__ == "__main__":
    main()
