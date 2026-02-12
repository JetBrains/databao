import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.api import agent
from databao.configs.llm import LLMConfig
from databao.core import (
    Agent,
    Context,
    ContextBuilder,
    ExecutionResult,
    Executor,
    Opa,
    SourcesManager,
    Thread,
    VisualisationResult,
    Visualizer,
)
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime, supported_db_types

__all__ = [
    "Agent",
    "Context",
    "ContextBuilder",
    "DBConnection",
    "DBConnectionConfig",
    "DBConnectionRuntime",
    "ExecutionResult",
    "Executor",
    "LLMConfig",
    "Opa",
    "SourcesManager",
    "Thread",
    "VisualisationResult",
    "Visualizer",
    "__version__",
    "agent",
    "supported_db_types",
]
