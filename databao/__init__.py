import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.api import agent, domain
from databao.configs.llm import LLMConfig
from databao.core import (
    Agent,
    Domain,
    ExecutionResult,
    Executor,
    Opa,
    Thread,
    VisualisationResult,
    Visualizer,
)
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime

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
