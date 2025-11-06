# Databao: NL queries for data

Natural‑language queries for your data — connect SQL databases and DataFrames, ask questions in plain English, and get tables, plots, and explanations back. 
Databao runs agents on top of dataframes and your DB connections, and can use both cloud and local LLMs.


## Overview
- Ask questions like “list all German shows” or “plot revenue by month”.
- Works with SQLAlchemy engines (e.g., Postgres) and in‑memory DataFrames via a session.
- Built‑in visualization via a Vega‑Lite chat visualizer.
- Pluggable LLMs: OpenAI/Anthropic, or local models through Ollama or any OpenAI‑compatible server.


## Installation
Using pip:
```bash
pip install databao
```

## Quickstart

### 1) Create a database connection (SQLAlchemy)
```python
from sqlalchemy import create_engine

user = os.environ.get("DATABASE_USER")
password = os.environ.get("DATABASE_PASSWORD")
host = os.environ.get("DATABASE_HOST")
database = os.environ.get("DATABASE_NAME")

engine = create_engine(
    f"postgresql://{user}:{password}@{host}/{database}"
)
```

### 2) Open a databao session and register sources

```python
import databao 
from databao.configs.llm import LLMConfig

# Option A - Local: install and run any compatible local LLM. For list of compatible models, see: "Local models" below 
# llm = LLMConfig(name="ollama:gpt-oss:20b", temperature=0)

# Option B - Cloud (requires an API key, e.g. OPENAI_API_KEY)
llm_config = LLMConfig(name="gpt-4o-mini", temperature=0)
session = databao.open_session(name="demo", llm_config=llm_config)

# Add your database to the session
session.add_db(engine)
```

### 3) Ask questions and materialize results

```python
# Start a conversational thread
thread = session.thread()

# Ask a question and get a DataFrame
df = thread.ask("list all german shows").df()
print(df.head())

# Get a textual answer
print(thread.text())

# Generate a visualization (Vega-Lite under the hood)
plot = thread.plot("bar chart of shows by country")
print(plot.code)  # access generated plot code if needed
```

## Environment variables

Specify your API keys in the environment variables:
- `OPENAI_API_KEY` — if using OpenAI models
- `ANTHROPIC_API_KEY` — if using Anthropic models
- Optional for local/OAI‑compatible servers:
  - `OPENAI_BASE_URL` (aka `api_base_url` in code)
  - `OLLAMA_HOST` (e.g., `127.0.0.1:11434`)

## Local models
Databao can be used with local LLMs either using Ollama or OpenAI‑compatible servers (LM Studio, llama.cpp, etc.).

### Ollama
1. Install Ollama for your OS and make sure it is running.
2. Use an `LLMConfig` with `name` of the form `"ollama:<model_name>"`.
   For an example see `examples/configs/qwen3-8b-ollama.yaml`.

The model will be downloaded automatically if it doesn't already exist. Alternatively, run `ollama pull <model_name>` to download it manually.

### OpenAI‑compatible servers
You can use any OpenAI‑compatible server by setting `api_base_url` in the `LLMConfig`.
For an example, see `examples/configs/qwen3-8b-oai.yaml`.

Examples of compatible servers:
- LM Studio (macOS‑friendly; supports the OpenAI Responses API)
- Ollama (`OLLAMA_HOST=127.0.0.1:8080 ollama serve`)
- llama.cpp (`llama-server`)
- vLLM



## Development

Installation using uv (for development):

Clone this repo and run:
```bash
# Install dependencies for the library
uv sync

# Optionally include example extras (notebooks, dotenv)
uv sync --extra examples
```


Using Makefile targets:

```bash
# Lint and static checks (pre-commit on all files)
make check

# Run tests (loads .env if present)
make test
```

Using uv directly:

```bash
uv run pytest -v
uv run pre-commit run --all-files
```

### Tests
- Test suite uses `pytest`.
- Some tests are marked `@pytest.mark.apikey` and require provider API keys.

Run all tests:

```bash
uv run pytest -v
```

Run only tests that do NOT require API keys:

```bash
uv run pytest -v -m "not apikey"
```

### Project structure
```
databao/
  api.py                 # public entry: open_session(...)
  core/                  # Session, Pipe, Executor, Visualizer abstractions
  agents/                # Lighthouse (default) and React-DuckDB agents
  duckdb/                # DuckDB integration and tools
  visualizers/           # Vega-Lite chat visualizer and utilities
examples/                # notebooks, demo script, configs
tests/                   # pytest suite
```

### Entry points
- Programmatic: `from databao.api import open_session`
