import importlib.metadata

try:
    __version__ = importlib.metadata.version("databao-agent")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.agent.api import agent, domain
from databao.agent.configs.llm import LLMConfig
from databao.agent.core import (
    Agent,
    Domain,
    ExecutionResult,
    Executor,
    Opa,
    Thread,
    VisualisationResult,
    Visualizer,
)
from databao.agent.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime

__all__ = [
    "Agent",
    "DBConnection",
    "DBConnectionConfig",
    "DBConnectionRuntime",
    "Domain",
    "ExecutionResult",
    "Executor",
    "LLMConfig",
    "Opa",
    "Thread",
    "VisualisationResult",
    "Visualizer",
    "__version__",
    "agent",
    "domain",
]
