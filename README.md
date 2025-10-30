# Portus: NL queries for data

Natural‑language queries for your data — connect SQL databases and DataFrames, ask questions in plain English, and get tables, plots, and explanations back. Portus runs agents on top of DuckDB and your connections, and can use both cloud and local LLMs.

Version: 0.0.4

## Overview
- Ask questions like “list all German shows” or “plot revenue by month”.
- Works with SQLAlchemy engines (e.g., Postgres) and in‑memory DataFrames via a session.
- Uses DuckDB under the hood to federate/query data.
- Built‑in visualization via a Vega‑Lite chat visualizer.
- Pluggable LLMs: OpenAI/Anthropic, or local models through Ollama or any OpenAI‑compatible server.

## Stack and Tooling
- Language: Python (>= 3.12)
- Packaging/build: Hatchling
- Package manager: uv (required version 0.9.5)
- Key deps: pandas, duckdb, langchain, langgraph, jinja2, psycopg2‑binary, matplotlib, edaplot‑vl
- Dev tools: ruff, mypy, pytest, pre‑commit

## Requirements
- Python 3.12+
- uv 0.9.5 (see pyproject `[tool.uv]`)
- Optional: Postgres client libs when using `psycopg2-binary` on your platform

## Installation
Using uv (recommended):

```bash
# Install dependencies for the library
uv sync

# Optionally include example extras (notebooks, dotenv)
uv sync --extra examples
```

If you are developing, you may also want the `dev` dependency group:

```bash
uv sync --group dev
```

## Environment variables
Copy `.env.example` to `.env` and fill in your keys:

```bash
cp .env.example .env
```

Supported variables:
- `OPENAI_API_KEY` — if using OpenAI models
- `ANTHROPIC_API_KEY` — if using Anthropic models
- Optional for local/OAI‑compatible servers:
  - `OPENAI_BASE_URL` (aka `api_base_url` in code)
  - `OLLAMA_HOST` (e.g., `127.0.0.1:11434`)

## Quickstart

### 1) Create a database connection (SQLAlchemy)
Do not hard‑code credentials in code. Use env vars or a secret manager. Example with placeholders:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://<user>:<password>@<host>/<database>"
)
```

### 2) Open a Portus session and register sources

```python
from portus.api import open_session
from portus.configs.llm import LLMConfig

llm_config = LLMConfig(name="gpt-4o-mini", temperature=0)
session = open_session(name="demo", llm_config=llm_config)

# Register your engine (also supports native DuckDB connections)
session.add_db(engine, context="Postgres database for streaming catalog")
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

## Local models
Portus can be used with local LLMs either using Ollama or OpenAI‑compatible servers (LM Studio, llama.cpp, vLLM, etc.).

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

## Scripts and common tasks
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

## Tests
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

## Project structure
```
portus/
  api.py                 # public entry: open_session(...)
  core/                  # Session, Pipe, Executor, Visualizer abstractions
  agents/                # Lighthouse (default) and React-DuckDB agents
  duckdb/                # DuckDB integration and tools
  visualizers/           # Vega-Lite chat visualizer and utilities
examples/                # notebooks, demo script, configs
tests/                   # pytest suite
```

## Entry points
- Programmatic: `from portus.api import open_session`
- CLI: none at the moment. TODO: consider adding a CLI entry point via `[project.scripts]`.

## License
TODO: Add a LICENSE file and state the license here.

## Notes
- This README was updated on 2025-10-30 to remove hard‑coded credentials and reflect the current stack (uv + hatchling).
- If anything here is inaccurate or missing (e.g., additional environment variables, supported backends), please open an issue or a PR.
