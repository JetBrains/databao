import pandas as pd
import logging
from sqlalchemy import create_engine
from langchain_openai import ChatOpenAI
import portus

logging.basicConfig(level=logging.INFO)

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix"
)

df = pd.read_sql("""
                 SELECT *
                 FROM netflix_shows
                 WHERE country = 'Germany'
                 """, engine)
print(df)

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
session = portus.create_session(llm)
session.add_db(engine)

print(session.ask("list all german shows").df())
