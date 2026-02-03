import logging
from pathlib import Path

import duckdb
import databao
from databao import LLMConfig
from databao.dbt import DbtConfig
from databao.executors.dbt.executor import DbtProjectExecutor

logging.basicConfig(level=logging.INFO)

DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/databao-agent/examples/shopify001/shopify.duckdb"
DBT_SOURCE_PROJ_PATH = Path("/Users/andrei.gasparian/Documents/spider-2.0/databao-agent/examples/shopify001")

conn = duckdb.connect(str(DB_PATH))

llm_config = LLMConfig(name="gpt-5", temperature=0, agent_recursion_limit=400)

agent = databao.new_agent(
    name="demo-dbt-executor",
    llm_config=llm_config,
    dbt_config=DbtConfig(project_dir=DBT_SOURCE_PROJ_PATH),
    data_executor=DbtProjectExecutor(dbt_config=DbtConfig(project_dir=DBT_SOURCE_PROJ_PATH)),
)

agent.add_db(conn)

thread = agent.thread(stream_ask=True)

thread.ask(
    "What percentage of orders are fulfilled within 48 hours"
)

print("\n=== TEXT ===\n")
print(thread.text())

print("\n=== CODE ===\n")
print(thread.code())
