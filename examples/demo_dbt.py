import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv

import databao.agent as bao
from databao.agent.configs.agent import AgentConfig
from databao.agent.databases import DuckDBConnectionConfig
from databao.agent.executors.dbt import DbtProjectExecutor
from databao.agent.executors.query_expansion import QueryExpansionConfig

load_dotenv()

logging.basicConfig(level=logging.INFO)

EXAMPLES_DIR = Path(__file__).resolve().parent
# NOTE: (@gas) in order to build the context with DCE,
# dbt project should be "initialized", e.g. with `dbt run`;
# the demo project is taken from the Spider-2-dbt dataset
DBT_PROJ_PATH = EXAMPLES_DIR / "shopify002"
DB_PATH = DBT_PROJ_PATH / "shopify.duckdb"

llm_config = bao.LLMConfig(name="gpt-5", temperature=0)
agent_config = AgentConfig(recursion_limit=100, parallel_tool_calls=True)

with tempfile.TemporaryDirectory(prefix="dbt-agent-") as tmp_dce_proj_dir:
    domain_ctx = bao.domain(project_dir=tmp_dce_proj_dir)

    duckdb_config = DuckDBConnectionConfig(database_path=str(DB_PATH))
    domain_ctx.add_db(duckdb_config, name="shopify", description="Shopify e-commerce data")
    domain_ctx.add_dbt(DBT_PROJ_PATH, name="shopify-dbt", description="dbt transformations project")

    domain_ctx.build_context()  # explicit call is optional

    expansion_config = QueryExpansionConfig(num_queries=2, rrf_k=60)
    agent = bao.agent(
        domain=domain_ctx,
        name="demo-dbt-executor",
        llm_config=llm_config,
        agent_config=agent_config,
        data_executor=DbtProjectExecutor(expansion_config=expansion_config),
        # NOTE: (@gas) you can disable query expansion just not providing it for the executor
        # data_executor=DbtProjectExecutor(),
    )

    thread = agent.thread(stream_ask=True)

    thread.ask(
        "What is our refund rate by month?",
        metadata={"source": "shopify-dbt"},
    )

    print("\n=== TEXT ===\n")
    print(thread.text())

    print("\n=== CODE ===\n")
    print(thread.code())

    print("\n=== Dataframe ===\n")
    print(thread.df())
