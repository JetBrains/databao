import logging
import tempfile
from pathlib import Path

from databao_context_engine import DatasourceType
from dotenv import load_dotenv

import databao
from databao import LLMConfig
from databao.api import domain
from databao.configs.agent import AgentConfig
from databao.databases import DBConnectionConfig
from databao.executors.dbt import DbtProjectExecutor
from databao.executors.query_expansion import QueryExpansionConfig

load_dotenv()

logging.basicConfig(level=logging.INFO)

EXAMPLES_DIR = Path(__file__).resolve().parent
# NOTE: (@gas) in order to build the context with DCE,
# dbt project should be "initialized", e.g. with `dbt run`;
# the demo project is taken from the Spider-2-dbt dataset
DBT_PROJ_PATH = EXAMPLES_DIR / "shopify002"
DB_PATH = DBT_PROJ_PATH / "shopify.duckdb"

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent_config = AgentConfig(recursion_limit=100, parallel_tool_calls=True)

with tempfile.TemporaryDirectory(prefix="dbt-agent-") as tmp_dce_proj_dir:
    domain_ctx = domain(project_dir=tmp_dce_proj_dir)

    duckdb_config = DBConnectionConfig(
        type=DatasourceType(full_type="duckdb"),
        content={"database_path": str(DB_PATH)},
    )
    domain_ctx.add_source(duckdb_config, name="shopify", description="Shopify e-commerce data")

    dbt_config = DBConnectionConfig(
        type=DatasourceType(full_type="dbt"),
        content={"dbt_target_folder_path": str(DBT_PROJ_PATH / "target")},
    )
    domain_ctx.add_source(dbt_config, name="shopify-dbt", description="dbt transformations project")

    domain_ctx.finalize_sources()  # type: ignore
    domain_ctx.build_context()

    expansion_config = QueryExpansionConfig(num_queries=2, rrf_k=60)
    agent = databao.agent(
        domain=domain_ctx,
        name="demo-dbt-executor",
        llm_config=llm_config,
        agent_config=agent_config,
        data_executor=DbtProjectExecutor(expansion_config=expansion_config),
        # NOTE: (@gas) you can disable query expansion just not providing it for the executor
        # data_executor=DbtProjectExecutor(),
    )

    thread = agent.thread(stream_ask=True)

    thread.ask("What is our refund rate by month?")

    print("\n=== TEXT ===\n")
    print(thread.text())

    print("\n=== CODE ===\n")
    print(thread.code())

    print("\n=== Dataframe ===\n")
    print(thread.df())
