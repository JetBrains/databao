import logging

import pandas as pd
from sqlalchemy import create_engine

import portus

logging.basicConfig(level=logging.INFO)

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

session = portus.open_session("my_session")
session.add_db(engine)

data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
df = pd.DataFrame(data)
session.add_df(df)

ask = session.ask("count cancelled shows by directors")
df_result = ask.df
if df_result is not None:
    print(df_result)
plot = ask.plot
if hasattr(ask, "code"):
    print(ask.code)
