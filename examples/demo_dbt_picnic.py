import logging
from pathlib import Path

import duckdb
import databao
from databao import LLMConfig
from databao.dbt import DbtConfig
from databao.executors.dbt.executor import DbtProjectExecutor

logging.basicConfig(level=logging.INFO)

DB_PATH = "/Users/andrei.gasparian/Documents/databao-agent/examples/shopify002/shopify.duckdb"
DBT_PROJ_PATH = Path("/Users/andrei.gasparian/Documents/databao-agent/examples/shopify002")

llm_config = LLMConfig(name="gpt-5", temperature=0, agent_recursion_limit=400)

agent = databao.new_agent(
    name="demo-dbt-executor",
    llm_config=llm_config,
    data_executor=DbtProjectExecutor(dbt_config=DbtConfig(project_dir=DBT_PROJ_PATH)),
)

conn = duckdb.connect(DB_PATH)
agent.add_db(conn)

thread = agent.thread(stream_ask=True)

# "What is our 90-day repeat purchase rate"
thread.ask(
    "What share of orders use a discount code (Discount attach rate)"
)

print("\n=== TEXT ===\n")
print(thread.text())

print("\n=== CODE ===\n")
print(thread.code())
