import logging
from pathlib import Path

import duckdb
import databao
from databao import LLMConfig, Context
from databao.configs.agent import AgentConfig
from databao.executors.dbt import DbtConfig, DbtProjectExecutor

logging.basicConfig(level=logging.INFO)

DB_PATH = "/Users/andrei.gasparian/Documents/databao-agent/examples/shopify002/shopify.duckdb"
DBT_PROJ_PATH = Path("/Users/andrei.gasparian/Documents/databao-agent/examples/shopify002")

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent_config = AgentConfig(recursion_limit=100, parallel_tool_calls=True)

context_builder = Context.builder()

engine = duckdb.connect(DB_PATH)
context_builder.add_db(engine)

context = context_builder.build()

agent = databao.agent(
    context=context,
    name="demo-dbt-executor",
    llm_config=llm_config,
    agent_config=agent_config,
    data_executor=DbtProjectExecutor(
        dbt_config=DbtConfig(
            project_dir=DBT_PROJ_PATH,
        ),
        use_sandbox=False,
    ),
)

thread = agent.thread(stream_ask=True)

# "What is our 90-day repeat purchase rate"
# "What share of orders use a discount code (Discount attach rate)"
# "What is our abandoned checkout recovery rate within 7 days"
# "How long does it take to fulfill an order?"
thread.ask(
    "What is our refund rate by month?"
)

print("\n=== TEXT ===\n")
print(thread.text())

print("\n=== CODE ===\n")
print(thread.code())

print("\n=== Dataframe ===\n")
print(thread.df())
