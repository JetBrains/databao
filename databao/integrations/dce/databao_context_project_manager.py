from pathlib import Path

from databao_context_engine import (
    BuildDatasourceResult,
    ConfiguredDatasource,
    DatabaoContextDomainManager,
    Datasource,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile


class DatabaoContextProjectManagerApi:
    def __init__(self, delegate: DatabaoContextDomainManager):
        self._delegate = delegate

    def create_datasource_config(self, config_file: AbstractConfigFile) -> ConfiguredDatasource:
        datasource_type = DatasourceType(full_type=config_file.type)
        datasource_name = config_file.name
        return self._delegate.create_datasource_config(datasource_type, datasource_name, config_file)

    def get_configured_datasource_list(self) -> list[ConfiguredDatasource]:
        return self._delegate.get_configured_datasource_list()

    # TODO (dce): should be present only in DatabaoContextEngineApi
    def get_introspected_datasource_list(self) -> list[Datasource]:
        return self._delegate.get_engine_for_domain().get_introspected_datasource_list()

    def build_context(self) -> list[BuildDatasourceResult]:
        return self._delegate.build_context()

    # TODO (dce): should be provided by the DCE side
    @staticmethod
    def _get_datasource_name(datasource_id: DatasourceId) -> str:
        return datasource_id.datasource_path.split("/")[-1]

    @property
    def project_dir(self) -> Path:
        return self._delegate.domain_dir
