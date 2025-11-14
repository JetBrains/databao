import importlib.metadata

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode


from databao.api import open_session
from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, Executor, Opa, Pipe, Session, VisualisationResult, Visualizer

__all__ = [
    "ExecutionResult",
    "Executor",
    "LLMConfig",
    "Opa",
    "Pipe",
    "Session",
    "VisualisationResult",
    "Visualizer",
    "__version__",
    "open_session",
]
