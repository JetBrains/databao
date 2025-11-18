import logging

import pandas as pd
from sqlalchemy import create_engine

import databao

logging.basicConfig(level=logging.WARNING)

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix?options=endpoint%3Dep-young-breeze-a5cq8xns&sslmode=require"
)

df = pd.read_sql(
    """
                 SELECT *
                 FROM netflix_shows
                 WHERE country = 'Germany'
                 """,
    engine,
)
print(df)

# llm_config = LLMConfig.from_yaml("configs/qwen3-8b-ollama.yaml")  # Use a custom config file
# llm_config = LLMConfigDirectory.QWEN3_8B_OLLAMA  # Use one of the preconfigured configs
llm_config = None  # Omit the config to use the default config
agent = databao.new_agent("my_agent", llm_config=llm_config)
agent.add_db(engine)

data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
df = pd.DataFrame(data)
agent.add_df(df)

thread = agent.thread()
thread.ask("count cancelled shows by directors")
print(thread.text())
print(f"\n```\n{thread.code()}\n```\n")
df = thread.df()
print(f"\n{df.to_string() if df is not None else df}\n")

plot = thread.plot()
print(plot.text)