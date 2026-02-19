import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao_agent.api import agent
from databao_agent.configs.llm import LLMConfig
from databao_agent.core import (
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
from databao_agent.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime, supported_db_types

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
