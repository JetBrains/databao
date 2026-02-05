from pathlib import Path
from typing import Any

from databao_context_engine import (
    BuildContextResult,
    DatabaoContextProjectManager,
    DatasourceConfigFile,
    DatasourceType,
)
from databao_context_engine.datasources.datasource_discovery import discover_datasources, logger, prepare_source
from databao_context_engine.datasources.types import PreparedDatasource
from databao_context_engine.project.layout import ProjectLayout


class DatabaoContextProjectManagerApi:
    def __init__(self, delegate: DatabaoContextProjectManager):
        self._delegate = delegate

    def create_datasource_config(
        self, datasource_type: DatasourceType, datasource_name: str, config_content: dict[str, Any]
    ) -> DatasourceConfigFile:
        return self._delegate.create_datasource_config(datasource_type, datasource_name, config_content)

    # TODO (dce): should be implemented on the DCE side
    def get_prepared_datasource_list(self) -> list[PreparedDatasource]:
        result = []
        for discovered_datasource in discover_datasources(project_layout=self.project_layout):
            try:
                prepared_source = prepare_source(discovered_datasource)
            except Exception as e:
                logger.debug(str(e), exc_info=True, stack_info=True)
                logger.info(f"Invalid source at ({discovered_datasource.path}): {e!s}")
                continue
            result.append(prepared_source)
        return result

    def build_context(self) -> list[BuildContextResult]:
        return self._delegate.build_context()

    @property
    def project_dir(self) -> Path:
        return self._delegate.project_dir

    @property
    def project_layout(self) -> ProjectLayout:
        return self._delegate._project_layout
