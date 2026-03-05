import logging

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

import databao.agent as bao

load_dotenv()

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

domain = bao.domain()
domain.add_db(engine)

data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
df = pd.DataFrame(data)
domain.add_df(df)

# llm_config = bao.LLMConfig.from_yaml("configs/qwen3-8b-ollama.yaml")  # Use a custom config file
# llm_config = LLMConfigDirectory.QWEN3_8B_OLLAMA  # Use one of the preconfigured configs
llm_config = None  # Omit the config to use the default config
agent = bao.agent(domain, name="my_agent", llm_config=llm_config)

thread = agent.thread()
thread.ask(
    "count cancelled shows by directors",
    metadata={"source": "netflix"},
)

print(thread.text())
print(f"\n```\n{thread.code()}\n```\n")
df = thread.df()
print(f"\n{df.to_string() if df is not None else df}\n")

plot = thread.plot()
print(plot.text)
