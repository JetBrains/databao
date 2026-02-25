from typing import Any

from databao_context_engine import ContextSearchResult
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool, tool

from databao.core import Domain
from databao.core.domain import _DCEProjectDomain
from databao.executors.query_expansion import (
    QueryExpansionConfig,
    expand_queries,
    reciprocal_rank_fusion,
)
from databao.integrations.dce import DatabaoContextApi


def make_search_context_tool(
    domain: Domain,
    *,
    expansion_llm: BaseChatModel | None = None,
    expansion_config: QueryExpansionConfig | None = None,
) -> BaseTool | None:
    if not domain.supports_context:
        return None
    if isinstance(domain, _DCEProjectDomain):
        return _make_dce_search_context_tool(domain, expansion_llm=expansion_llm, expansion_config=expansion_config)
    raise ValueError(f"Search context tool is not supported for domain type: {type(domain)}")


def _make_dce_search_context_tool(
    domain: _DCEProjectDomain,
    *,
    expansion_llm: BaseChatModel | None = None,
    expansion_config: QueryExpansionConfig | None = None,
) -> BaseTool | None:
    if expansion_llm is not None and expansion_config is not None:
        return _make_dce_expanded_search_tool(domain, expansion_llm, expansion_config)
    return _make_dce_plain_search_tool(domain)


def _make_dce_plain_search_tool(domain: _DCEProjectDomain) -> BaseTool:
    """Build the search_context tool without query expansion."""

    @tool(parse_docstring=True)
    def search_context(retrieve_text: str, datasource_name: str | None = None) -> list[dict[str, Any]]:
        """Search the context for relevant information matching the given query text.

        Args:
            retrieve_text: Natural language query to search the context for relevant results.
            datasource_name: Optional datasource name to restrict the search to a specific data source.
        """
        search_result_list = domain.search_context(retrieve_text, datasource_name=datasource_name)
        return list(map(_search_result_to_dict, search_result_list))

    return search_context


def _make_dce_expanded_search_tool(
    domain: _DCEProjectDomain,
    expansion_llm: BaseChatModel,
    expansion_config: QueryExpansionConfig,
) -> BaseTool:
    """Build the search_context tool with LLM query expansion + RRF re-ranking."""

    @tool(parse_docstring=True)
    def search_context(
        retrieve_text: str,
        datasource_name: str | None = None,
        datasource_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search the context for relevant information matching the given query text.

        Internally expands the query into multiple retrieval-friendly variants adapted
        to the datasource naming conventions, then merges results via rank fusion.

        Args:
            retrieve_text: Natural language query to search the context for relevant results.
            datasource_name: Optional datasource name to restrict the search to a specific data source.
            datasource_type: Optional datasource type hint (e.g. "dbt", "snowflake", "postgres").
                Used to adapt query expansion to the naming conventions of the target system.
        """
        queries = expand_queries(
            retrieve_text,
            expansion_llm,
            expansion_config,
            datasource_type=datasource_type or "sql",
        )

        ranked_lists: list[list[dict[str, Any]]] = []
        for q in queries:
            results = domain.search_context(q, datasource_name=datasource_name)
            ranked_lists.append(list(map(_search_result_to_dict, results)))

        if len(ranked_lists) <= 1:
            return ranked_lists[0] if ranked_lists else []

        return reciprocal_rank_fusion(ranked_lists, k=expansion_config.rrf_k)

    return search_context


def _search_result_to_dict(search_result: ContextSearchResult) -> dict[str, Any]:
    return {
        "data_source_name": _get_ds_name(search_result),
        "distance": search_result.distance,
        "context_result": search_result.context_result,
    }


def _get_ds_name(search_result: ContextSearchResult) -> str:
    ds_id = search_result.datasource_id
    return DatabaoContextApi.get_datasource_name(ds_id)
