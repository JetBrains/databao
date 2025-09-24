# Portus: NL queries for data

## Setup connection

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix"
)
```

## Create portus session

```python
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
session = portus.open_session(llm)
session.add_db(engine)
```

## Query data

```python
session.ask("list all german shows").df()
```

