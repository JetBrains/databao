from pathlib import Path

from databao_context_engine import ContextSearchResult, DatabaoContextEngine


class DatabaoContextEngineApi:
    def __init__(self, delegate: DatabaoContextEngine):
        self._delegate = delegate

    def search_context(self, retrieve_text: str) -> list[ContextSearchResult]:
        return self._delegate.search_context(retrieve_text)

    @property
    def project_dir(self) -> Path:
        return self._delegate.project_dir
