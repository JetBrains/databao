import duckdb
import databao
from databao import LLMConfig

DB_PATH = "/Users/andrei.gasparian/Documents/spider-2.0/Spider2/spider2-dbt/examples/shopify001/shopify.duckdb"

conn = duckdb.connect(DB_PATH)

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent = databao.new_agent(name="demo", llm_config=llm_config)

agent.add_db(conn)
thread = agent.thread()

thread.ask(
    "Generate analytical questions that could lead to some metric generation, "
    "that you can base on tables and data available at the database shared with you."
    "Keep it simple: I need just 3 questions, with simple, explainable to human metrics, "
    "which are not exists in the shared database. "
    "Yet the answer *should be possible* to derive from that database."
)

print("--------")
print()
print(thread.text())
print("--------")
print()
print(thread.code())
