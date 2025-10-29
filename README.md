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
thread = session.thread()
thread.ask("list all german shows").df()
```


## Local models

Portus can be used with local LLMs either using ollama or OpenAI API compatible servers (LM Studio, llama.cpp, etc.).

### Ollama

1. Install [ollama](https://ollama.com/download) for your operating system and make sure it is running.
2. Use an LLMConfig with `name` of the form `ollama:model_name`. For an example see [qwen3-8b-ollama.yaml](examples/configs/qwen3-8b-ollama.yaml).

The model will be downloaded automatically if it doesn't already exist.
Alternatively, `ollama pull model_name` to download the model manually.

### OpenAI compatible servers

You can use any OAI compatible server by setting `api_base_url` in the LLMConfig. For an example, see [qwen3-8b.yaml](examples/configs/qwen3-8b-oai.yaml).

Examples of OAI compatible servers:
- [LM Studio](https://lmstudio.ai/) - Recommended for macOS (LMX engine for M-based chips, supports the [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses)).
- [ollama](https://ollama.com/) - Run with `OLLAMA_HOST=127.0.0.1:8080 ollama serve`. We recommend using ollama directly, as described [above](#ollama).
- [llama.cpp](https://github.com/ggml-org/llama.cpp/tree/master/tools/server) using `llama-server`
- [vLLM](https://github.com/vllm-project/vllm)
- etc.


## Dev environment

We recommend using [uv](https://docs.astral.sh/uv/) as the package manager.

```bash
# Install dependencies
uv sync --all-extras

# Install pre-commit hooks for development
uv run pre-commit install
```
