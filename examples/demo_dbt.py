import logging
logging.basicConfig(level=logging.DEBUG)

import duckdb
import databao
from databao import LLMConfig

from dbt_agent_tmp.langchain_agent import LangchainAgent


# partially completed dbtv2 proj (copy from the original Spider2-dbtv2 dataset)
DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/Spider2/spider2-dbt/examples/shopify001/shopify.duckdb"
DBT_PROJ_PATH = "/Users/andrei.gasparian/Documents/databao-agent/examples/shopify001"

conn = duckdb.connect(DB_PATH)

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent = databao.new_agent(name="demo-dbtv2", llm_config=llm_config)

agent.add_db(conn)
thread = agent.thread()

df = thread.ask("generate customers cohorts").df()
print(df.head())
print()
print("--------")
print()
print(thread.text())

"""
use-case: 
  - user can ask agent so generated metric could be better fit to the real life
  - maybe user fixes could be additionally trigger "memorize" or some other new method so dbtv2 agent could use it
  - them summarize the hwole thing (history + user hints)
  - then dbtv2 agent kicks in, uses that summary + memorized hints (?)
  - add ability to materialize dbtv2 models separately: .confirm()? or .materialize()?
  
technical:
  - 
"""

# TODO: (@gas) could summary be replaced with thread.text() + thread.code()?
thread.ask("summarize you solution into the brief description of what's has been done supplying it with the final set of sql queries, needed to reproduce the result in the future")
print()
print("--------")
print()
print(thread.text())


task_prompt = "Given the following summary, add missing data models:\n" + thread.text()

dbt_agent = LangchainAgent(
    name="langchain_dbt_agent",
    project_dir=DBT_PROJ_PATH,
    system_prompt_name="system_prompt.jinja",
    db_conn=conn,
)
output = dbt_agent.run(task_prompt)
print()
print("--------")
print()
print(output)

# ---
# # Generate a visualization (Vega-Lite under the hood)
# plot = thread.plot("bar chart of shows by country")
# print(plot.code)  # access generated plot code if needed