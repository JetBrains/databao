import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.api import new_agent
from databao.configs.llm import LLMConfig
from databao.core import Agent, ExecutionResult, Executor, Opa, Thread, VisualisationResult, Visualizer

__all__ = [
    "Agent",
    "ExecutionResult",
    "Executor",
    "LLMConfig",
    "Opa",
    "Thread",
    "VisualisationResult",
    "Visualizer",
    "__version__",
    "new_agent",
]
