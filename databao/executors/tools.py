from typing import Any

from databao_context_engine import ContextSearchResult
from langchain_core.tools import BaseTool, tool

from databao.core import Domain
from databao.core.domain import _PersistentDomain
from databao.integrations.dce import DatabaoContextApi


def make_search_context_tool(domain: Domain) -> BaseTool | None:
    if not isinstance(domain, _PersistentDomain):
        return None

    @tool(parse_docstring=True)
    def search_context(retrieve_text: str) -> list[dict[str, Any]]:
        """Search the context for relevant information matching the given query text.

        Args:
            retrieve_text: Natural language query to search the context for relevant results.
        """
        search_result_list = domain.search_context(retrieve_text)
        return list(map(_search_result_to_dict, search_result_list))

    def _search_result_to_dict(search_result: ContextSearchResult) -> dict[str, Any]:
        result = {
            "data_source_name": _get_ds_name(search_result),
            "distance": search_result.distance,
            "context_result": search_result.context_result,
        }
        return result

    def _get_ds_name(search_result: ContextSearchResult) -> str:
        ds_id = search_result.datasource_id
        return DatabaoContextApi.get_datasource_name(ds_id)

    return search_context
