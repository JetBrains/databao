import logging
from pathlib import Path

import databao
from databao import LLMConfig
from databao.dbt import DbtConfig
from databao.executors.dbt.executor import DbtProjectExecutor
from databao.duckdb.types import make_duckdb_factory

logging.basicConfig(level=logging.INFO)

DB_PATH = "/Users/andrei.gasparian/Documents/databao-agent/examples/shopify001/shopify.duckdb"
DBT_PROJ_PATH = Path("/Users/andrei.gasparian/Documents/databao-agent/examples/shopify001")

llm_config = LLMConfig(name="gpt-5", temperature=0, agent_recursion_limit=400)

agent = databao.new_agent(
    name="demo-dbt-executor",
    llm_config=llm_config,
    data_executor=DbtProjectExecutor(dbt_config=DbtConfig(project_dir=DBT_PROJ_PATH)),
)

agent.add_db(make_duckdb_factory(DB_PATH))

thread = agent.thread(stream_ask=True)

thread.ask(
    "What percentage of orders are fulfilled within 48 hours"
)

print("\n=== TEXT ===\n")
print(thread.text())

print("\n=== CODE ===\n")
print(thread.code())
