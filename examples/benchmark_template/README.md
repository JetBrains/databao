# Databao Benchmark Template

Benchmark [databao](https://github.com/JetBrains/databao-agent) on your own database with your own gold SQL queries. See how well it answers natural-language questions against your data.

Tested with PostgreSQL, Snowflake, and DuckDB.

## Quick Start

```bash
# Install uv if you don't have it:
# curl -LsSf https://astral.sh/uv/install.sh | sh

uv sync --extra databao
cp .env.template .env
# Edit .env — set at least OPENAI_API_KEY and your database credentials
```

### 1. Configure your database

Edit `.env` and fill in your values:

**PostgreSQL / MySQL / BigQuery** -- set `DATABASE_TYPE=sqlalchemy`:

```dotenv
DATABASE_TYPE=sqlalchemy
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

Install your driver: `uv add psycopg2-binary` (Postgres), `uv add pymysql` (MySQL), etc.

**DuckDB** -- set `DATABASE_TYPE=duckdb`:

```dotenv
DATABASE_TYPE=duckdb
DUCKDB_PATH=/path/to/your/database.duckdb
```

**Snowflake** -- set `DATABASE_TYPE=snowflake`:

```bash
uv sync --extra databao --extra snowflake
```

***Required for databao:*** install the Snowflake ADBC driver so databao can query Snowflake. 

```bash
# Linux/macOS/WSL:
curl -sSL https://raw.githubusercontent.com/iqea-ai/duckdb-snowflake/main/scripts/install-adbc-driver.sh | sh
```

See [duckdb-snowflake ADBC driver instructions](https://github.com/iqea-ai/duckdb-snowflake?tab=readme-ov-file#adbc-driver-setup) for Windows or manual installation.

```dotenv
DATABASE_TYPE=snowflake
SNOWFLAKE_AUTH=key_pair
SNOWFLAKE_USER=my_user
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_DATABASE=MY_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=MY_WH
SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/my_key.pem
```

### 2. Add your gold SQL questions to `benchmark_questions.csv`

```csv
user_input,gold_sql,difficulty
How many orders are there?,SELECT COUNT(*) FROM orders,easy
What is the total revenue?,SELECT SUM(amount) FROM payments,medium
```

### 3. Run

```bash
uv run examples/databao_benchmark.py
```

That's it. The benchmark will run each question through databao, execute both the gold SQL and the generated SQL against your database, and score the results.

### CLI options

```bash
uv run examples/databao_benchmark.py --limit 5          # run first 5 questions only
uv run examples/databao_benchmark.py --sql-model gpt-5.4 # change the LLM model
uv run examples/databao_benchmark.py --judge-model gpt-5.4  # change the judge model
```

## Benchmarking your own agent

If you want to benchmark a different text-to-SQL agent (not databao), use `examples/your_benchmark.py` as a starting point. Implement the `predict_fn`:

```python
async def predict_fn(question: str) -> tuple[bool, str | None, pd.DataFrame | str]:
    # Call your agent, return (success, generated_sql, result_dataframe_or_error)
    ...
```

Then run:

```bash
uv run examples/your_benchmark.py
```

## Configuration

All settings are read from environment variables (`.env` file). See `.env.template` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_TYPE` | `sqlalchemy` | `"sqlalchemy"`, `"duckdb"`, or `"snowflake"` |
| `DATABASE_URL` | `sqlite:///:memory:` | Connection string (for `sqlalchemy`) |
| `DUCKDB_PATH` | `""` | Path to .duckdb file (for `duckdb`) |
| `SNOWFLAKE_*` | `""` | Snowflake connection settings (for `snowflake`) |
| `INPUT_CSV` | `benchmark_questions.csv` | Path to gold SQL CSV |
| `OUTPUT_CSV` | `results/output.csv` | Where to write results |
| `JUDGE_MODEL` | `gpt-5.4` | OpenAI model for the LLM judge |
| `MAX_CONCURRENT` | `8` | Max concurrent benchmark queries |
| `DATASET_NAME` | `my_benchmark` | Name for Ragas experiment tracking |

## Metrics

**Execution Accuracy** -- Compares the predicted DataFrame to the gold DataFrame using [datacompy](https://github.com/capitalone/datacompy). Returns `correct` if data matches (with tolerance for small numeric differences).

**LLM Judge** -- Sends both DataFrames to an LLM that returns `correct`, `partially`, or `wrong`. Catches cases where data is semantically correct but structured differently.

## Enabling LangSmith Traces

Set the `LANGSMITH_*` variables in your `.env` file and install:

```bash
uv sync --extra databao --extra langsmith
```

LLM judge calls are automatically traced when LangSmith is configured.

## Project Structure

```
.env.template                # All env vars with comments
benchmark_questions.csv      # Your gold SQL questions
pyproject.toml
benchmark/
  core.py                    # Benchmark orchestration, CLI, summary
  db.py                      # Database runners + create_runner() factory
  metrics.py                 # LLM judge + execution accuracy
  helpers.py                 # Utilities
examples/
  databao_benchmark.py       # Databao benchmark (primary)
  your_benchmark.py          # Template for custom agents
```
