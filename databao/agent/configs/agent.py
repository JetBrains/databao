from pathlib import Path

from pydantic import BaseModel, ConfigDict


class AgentConfig(BaseModel):
    """Configuration for databao agent."""

    recursion_limit: int = 50
    """Maximum recursion depth for LLM agent execution."""

    parallel_tool_calls: bool = True
    """Whether agent is allowed to call several tools in one response.
    Supported by OpenAI models only."""

    dbt_path: Path | None = None
    """Path to a DBT project."""

    model_config = ConfigDict(frozen=True, extra="forbid")


DEFAULT_AGENT_CONFIG = AgentConfig()
