# Portus: NL queries for data

## Setup connection

Create a `.env` file in the project root from `.env.template` file.

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

## Installation & Setup

```bash
# Install dependencies
uv sync

# Install pre-commit hooks for development
uv run pre-commit install
```