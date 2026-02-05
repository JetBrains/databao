import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.api import new_agent, new_agent_v2
from databao.configs.llm import LLMConfig
from databao.core import Agent, AgentV1, ExecutionResult, Executor, Opa, Thread, VisualisationResult, Visualizer
from databao.core.v2 import AgentV2, Context, ContextBuilder
from databao.databases import DBConnection, DBConnectionConfig, DBConnectionRuntime, supported_db_types

__all__ = [
    "Agent",
    "AgentV1",
    "AgentV2",
    "Context",
    "ContextBuilder",
    "DBConnection",
    "DBConnectionConfig",
    "DBConnectionRuntime",
    "ExecutionResult",
    "Executor",
    "LLMConfig",
    "Opa",
    "Thread",
    "VisualisationResult",
    "Visualizer",
    "__version__",
    "new_agent",
    "new_agent_v2",
    "supported_db_types",
]
