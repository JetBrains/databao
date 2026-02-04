[![official project](https://jb.gg/badges/official.svg)](https://github.com/JetBrains#jetbrains-on-github)
[![PyPI version](https://img.shields.io/pypi/v/databao.svg)](https://pypi.org/project/databao)
[![Python versions](https://img.shields.io/pypi/pyversions/databao.svg)](https://pypi.org/project/databao/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/JetBrains/databao/blob/main/LICENSE)
[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1zYAlVbuOfIA3Ux5LVahM2eBU7wJplO48?usp=sharing)

<h1 align="center">Databao Agent</h1>

<p align="center">
 <b>Talk to your data in plain English.</b><br/>
 Ask questions → Get answers (Text, SQL, and interactive visual insights).
</p>

<p align="center">
 <a href="https://databao.app">Website</a> •
 <a href="#quickstart">Quickstart</a> •
 <a href="#local-models">Local models</a> •
 <a href="#contributing">Contributing</a> •
 <a href="https://discord.gg/hEUqCcWdVh">Discord</a>
</p>

---

## What is Databao Agent?

Databao Agent is an **open-source AI agent** that lets you query your data sources using natural language. 

Simply ask:
- *"Show me all German shows"*
- *"Plot revenue by month"*
- *"Which customers churned last quarter?"*

Get back **tables, charts, and explanations** — no SQL or code needed.

<p align="center">
 <img src="https://databao.app/agent.png" alt="Databao Agent Demo" width="500">
</p>

## Why choose Databao Agent?

| Feature                   | What it means for you                                                |
|:--------------------------|:---------------------------------------------------------------------|
| **Interactive outputs**   | Tables you can sort/filter and charts you can zoom/hover (Vega-Lite) |
| **Simple, Pythonic API**  | `thread.ask("question").df()`just works                              |
| **Python-native**         | Fits perfectly into existing data science and exploratory workflows  |
| **Natural language**      | Ask questions about your data just like asking a colleague           |
| **Broad DB support**      | PostgreSQL, MySQL, SQLite, DuckDB... anything SQLAlchemy supports    |
| **Auto-generated charts** | Get Vega-Lite visualizations without writing plotting code           |
| **Local first**           | Use Ollama or LM Studio — your data never leaves your machine        |
| **Cloud LLM ready**       | Built-in support for OpenAI, Anthropic, and OpenAI-compatible APIs   |
| **Conversational**        | Maintains context for follow-up questions and iterative analysis     |

## Installation

```bash
pip install databao
```

## Supported data sources

* <img src="https://cdn.simpleicons.org/pandas/150458" width="16" height="16" alt=""> Pandas DataFrame
* <img src="https://cdn.simpleicons.org/postgresql/316192" width="16" height="16" alt=""> PostgreSQL
* <img src="https://cdn.simpleicons.org/mysql/4479A1" width="16" height="16" alt=""> MySQL
* <img src="https://cdn.simpleicons.org/sqlite/003B57" width="16" height="16" alt=""> SQLite
* <img src="https://cdn.simpleicons.org/duckdb/FFF000" width="16" height="16" alt=""> DuckDB

For PostgreSQL, MySQL, and SQLite, pass a SQLAlchemy `Engine` to `add_db()`. For DuckDB, pass `DuckDBPyConnection`.

## Quickstart

### 1. Create a database connection (SQLAlchemy)

```python
import os
from sqlalchemy import create_engine

user = os.environ.get("DATABASE_USER")
password = os.environ.get("DATABASE_PASSWORD")
host = os.environ.get("DATABASE_HOST")
database = os.environ.get("DATABASE_NAME")

engine = create_engine(
   f"postgresql://{user}:{password}@{host}/{database}"
)
```

### 2. Create a Databao agent and register sources

```python
import databao
from databao import LLMConfig

# Option A - Local: install and run any compatible local LLM
# For list of compatible models, see "Local Models" below
# llm_config = LLMConfig(name="ollama:gpt-oss:20b", temperature=0)

# Option B - Cloud (requires an API key, e.g. OPENAI_API_KEY)
llm_config = LLMConfig(name="gpt-4o-mini", temperature=0)
agent = databao.new_agent(name="demo", llm_config=llm_config)

# Add your database to the agent
agent.add_db(engine)
```

### 3. Ask questions and materialize results

```python
# Start a conversational thread
thread = agent.thread()

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

| Variable            | Description                                          |
|:--------------------|:-----------------------------------------------------|
| `OPENAI_API_KEY`    | Required for OpenAI models or OpenAI-compatible APIs |
| `ANTHROPIC_API_KEY` | Required for Anthropic models                        |

Optional for local/OpenAI-compatible servers:

| Variable          | Description                                     |
|:------------------|:------------------------------------------------|
| `OPENAI_BASE_URL` | Custom endpoint (aka `api_base_url` in code)    |
| `OLLAMA_HOST`     | Ollama server address (e.g., `127.0.0.1:11434`) |

## Local Models

Databao agent works great with local LLMs — your data never leaves your machine.

### Ollama

1. Install [Ollama](https://ollama.com/download) for your OS and make sure it’s running
2. Use an `LLMConfig` with `name` of the form `"ollama:<model_name>"`:

   ```python
   llm_config = LLMConfig(name="ollama:gpt-oss:20b", temperature=0)
   ```

   The model will be downloaded automatically if it doesn't exist. Or run `ollama pull <model_name>` to download manually.

### OpenAI-compatible servers

You can use any OpenAI-compatible server by setting `api_base_url` in the `LLMConfig`.

For an example, see `examples/configs/qwen3-8b-oai.yaml`.

**Compatible servers:**
* [LM Studio](https://lmstudio.ai/): macOS-friendly, supports OpenAI Responses API
* [Ollama](https://ollama.com/): `OLLAMA_HOST=127.0.0.1:8080 ollama serve`
* [llama.cpp](https://github.com/ggerganov/llama.cpp): `llama-server`
* [vLLM](https://github.com/vllm-project/vllm)

## Alternatives

How does Databao agent compare to other agentic data tools?

| Tool        | Open source | Local LLMs             | SQL + DataFrames | Multiple sources   | Interactive output |
|-------------|-------------|------------------------|------------------|--------------------|--------------------|
| **Databao** | ✅           | ✅ Native Ollama        | ✅ Both           | ✅ Multiple sources | ✅ Tables + charts  |
| PandasAI    | ✅           | ✅ Ollama/LM Studio     | ✅ Both           | ❌ One source       | ❌ Static           |
| Chat2DB     | ✅           | ✅ Custom LLM, SQL only | ❌ One DB         | ✅ Dashboards       |
| Vanna       | ✅           | ✅ Ollama               | SQL only         | ❌ One DB           | ✅ Plotly           |

## Development

### Installation (using uv)

Clone this repo and run:

```bash
# Install dependencies
uv sync

# Optionally include example extras (notebooks, dotenv)
uv sync --extra examples
```

We recommend using the same version of uv as GitHub Actions:

```bash
uv self update 0.9.5
```

### Makefile targets

```bash
# Lint and static checks (pre-commit on all files)
make check

# Run tests (loads .env if present)
make test
```

### Direct commands

```bash
uv run pytest -v
uv run pre-commit run --all-files
```

### Tests

The test suite uses pytest. Some tests require API keys and are marked with `@pytest.mark.apikey`.

```bash
# Run all tests
uv run pytest -v

# Run only tests that do NOT require API keys
uv run pytest -v -m "not apikey"
```

## Contributing

We love contributions! Here’s how you can help:

- ⭐ **Star this repo** — it helps others find us!
- 🐛 **Found a bug?** [Open an issue](https://github.com/JetBrains/databao-agent/issues)
- 💡 **Have an idea?** We’re all ears — create a feature request
- 👍 **Upvote issues** you care about — helps us prioritize
- 🔧 **Submit a PR**
- 📝 **Improve docs** — typos, examples, tutorials — everything helps!

New to open source? No worries! We’re friendly and happy to help you get started.

## License

Apache 2.0 — use it however you want. See the [LICENSE](LICENSE.md) file for details.

---

<p align="center">
 <b>Like Databao? </b> Give us a ⭐! It will help to distribute the technology.
</p>

<p align="center">
 <a href="https://databao.app">Website</a> •
 <a href="https://discord.gg/hEUqCcWdVh">Discord</a>
</p>
