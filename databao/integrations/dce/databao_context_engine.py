from pathlib import Path

from databao_context_engine import ContextSearchResult, DatabaoContextEngine, Datasource, DatasourceId


class DatabaoContextEngineApi:
    def __init__(self, delegate: DatabaoContextEngine):
        self._delegate = delegate

    def get_introspected_datasource_list(self) -> list[Datasource]:
        return self._delegate.get_introspected_datasource_list()

    # TODO (dce): should be implemented on the DCE side
    def is_context_built(self) -> bool:
        introspected_data_sources = self.get_introspected_datasource_list()
        return len(introspected_data_sources) > 0

    def search_context(
        self, retrieve_text: str, datasource_ids: list[DatasourceId] | None
    ) -> list[ContextSearchResult]:
        return self._delegate.search_context(retrieve_text, datasource_ids=datasource_ids)

    @property
    def project_dir(self) -> Path:
        return self._delegate.project_dir
