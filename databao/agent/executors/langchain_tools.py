from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool, tool

from databao.agent.core import Domain
from databao.agent.core.domain import _DCEProjectDomain
from databao.agent.executors.query_expansion import (
    QueryExpansionConfig,
)
from databao.agent.executors.utils import (
    search_context as _search_context,
)
from databao.agent.executors.utils import (
    search_context_with_query_expansion as _search_context_with_query_expansion,
)


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


# fmt: off
SEARCH_CONTEXT_TOOL_DESCRIPTION = \
"""Search the context for relevant information matching the given query text.

Use this tool to find additional information about the database (e.g., table and column descriptions) and
any attached data sources (e.g., dbt projects).

Prefer using this tool to get detailed database schema insights as opposed to running
your own database inspection SQL queries.

Your natural language query will be matched against a semantic and keyword based search index
to find relevant results. Include specific information in the query (e.g., table names, column names)
to get the best results.

Args:
    retrieve_text: Natural language query to search the context for relevant results.
"""
# fmt: on


def _make_dce_plain_search_tool(domain: _DCEProjectDomain) -> BaseTool:
    """Build the search_context tool without query expansion."""

    @tool(description=SEARCH_CONTEXT_TOOL_DESCRIPTION, parse_docstring=False)
    def search_context(
        retrieve_text: str,
    ) -> list[dict[str, Any]]:
        return _search_context(retrieve_text, domain=domain)

    return search_context


# fmt: off
SEARCH_CONTEXT_WITH_EXPANSION_TOOL_DESCRIPTION = \
"""Search the context for relevant information matching the given query text.
Internally expands the query into multiple retrieval-friendly variants adapted
to the datasource naming conventions, then merges results via rank fusion.

Args:
    retrieve_text: Natural language query to search the context for relevant results.
    datasource_name: Optional datasource name to restrict the search to a specific data source.
    datasource_type: Optional datasource type hint (e.g. "dbt", "snowflake", "postgres").
        Used to adapt query expansion to the naming conventions of the target system.
        """
# fmt: on


def _make_dce_expanded_search_tool(
    domain: _DCEProjectDomain,
    expansion_llm: BaseChatModel,
    expansion_config: QueryExpansionConfig,
) -> BaseTool:
    """Build the search_context tool with LLM query expansion + RRF re-ranking."""

    @tool(description=SEARCH_CONTEXT_WITH_EXPANSION_TOOL_DESCRIPTION, parse_docstring=False)
    def search_context(
        retrieve_text: str,
        datasource_name: str | None = None,
        datasource_type: str | None = None,
    ) -> list[dict[str, Any]]:
        return _search_context_with_query_expansion(
            retrieve_text,
            domain=domain,
            expansion_llm=expansion_llm,
            expansion_config=expansion_config,
            datasource_name=datasource_name,
            datasource_type=datasource_type,
        )

    return search_context
