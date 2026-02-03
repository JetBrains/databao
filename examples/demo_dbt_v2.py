import logging
from pathlib import Path

import duckdb
import databao
from databao import LLMConfig
from databao.dbtv2 import DbtConfig

logging.basicConfig(level=logging.DEBUG)

# partially completed dbtv2 proj (copy from the original Spider2-dbtv2 dataset)
DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/databao-agent/examples/shopify001/shopify.duckdb"
DBT_SOURCE_PROJ_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/databao-agent/examples/shopify001"

conn = duckdb.connect(DB_PATH)

llm_config = LLMConfig(name="gpt-5", temperature=0)

agent = databao.new_agent(
    name="demo-dbtv2",
    llm_config=llm_config,
    dbt_config=DbtConfig(project_dir=Path(DBT_SOURCE_PROJ_PATH)),
)

agent.add_db(conn)
thread = agent.thread()

df = thread.ask("generate customers cohorts").df()
print(df.head() if df is not None else df)
print("\n--------\n")
print(thread.text())

plan = thread.dbt_plan()

print("\n-------- DBT Plan\n")
plan.run(db_conn=conn)

print("\n--------\n")
print("DBT plan changes:")
print("  Added:", [str(p) for p in plan.added_files])
print("  Modified:", [str(p) for p in plan.modified_files])
print("  Deleted:", [str(p) for p in plan.deleted_files])

print("\n-------- Commit to source\n")
print(plan.commit())

print("\n-------- Applying to source\n")
print(plan.apply())
