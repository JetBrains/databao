import logging
import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

import databao.agent as bao
from databao.agent.configs.agent import AgentConfig
from databao.agent.databases import DuckDBConnectionConfig
from databao.agent.executors import ClaudeAgentExecutor

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

# Use a pre-built domain if DOMAIN_PATH is set, otherwise create a temporary one
domain_path = os.environ.get("DOMAIN_PATH")

if domain_path:
    domain_ctx = bao.domain(project_dir=domain_path)
else:
    tmp_dir = tempfile.mkdtemp(prefix="claude-agent-")
    domain_ctx = bao.domain(project_dir=tmp_dir)

    duckdb_config = DuckDBConnectionConfig(database_path=str(DB_PATH))
    domain_ctx.add_db(duckdb_config, name="shopify", description="Shopify e-commerce data")
    domain_ctx.add_dbt(DBT_PROJ_PATH, name="shopify_dbt", description="dbt transformations project")

    domain_ctx.build_context()

agent = bao.agent(
    domain=domain_ctx,
    name="demo-claude-agent",
    llm_config=llm_config,
    agent_config=agent_config,
    data_executor=ClaudeAgentExecutor(),
)

thread = agent.thread(stream_ask=True)

thread.ask(
    "What is our refund rate by month?",
    metadata={"source": "shopify_dbt"},
)

print("\n=== TEXT ===\n")
print(thread.text())

print("\n=== CODE ===\n")
print(thread.code())

print("\n=== Dataframe ===\n")
print(thread.df())
