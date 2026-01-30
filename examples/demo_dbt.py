import duckdb
import databao
from databao import LLMConfig

from dbt_agent.langchain_agent import LangchainAgent


# # dbt completed
# DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/out/shopify001/shopify.duckdb"

# partially complete dbt
DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/Spider2/spider2-dbt/examples/shopify001/shopify.duckdb"
DBT_PROJ_PATH = "/Users/andrei.gasparian/Documents/databao-agent/examples/shopify001"

conn = duckdb.connect(DB_PATH)

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent = databao.new_agent(name="demo-dbt", llm_config=llm_config)

agent.add_db(conn)
thread = agent.thread()

df = thread.ask("generate customers cohorts").df()
print(df.head())
print()
print("--------")
print()
print(thread.text())

# its a future node system message - summarize before passing to the dbt agent
thread.ask("summarize you solution into the brief description of what's has been done supplying it with the final set of sql queries, needed to reproduce the result in the future")
print()
print("--------")
print()
print(thread.text())

task_prompt = "Given the following summary, add missing data models:\n" + thread.text()


import logging
logging.basicConfig(level=logging.DEBUG)

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