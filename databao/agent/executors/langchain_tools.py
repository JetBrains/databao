from typing import Any, Literal

import yaml
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool, tool

from databao.agent.core import Domain
from databao.agent.core.domain import _DCEProjectDomain
from databao.agent.executors.query_expansion import (
    QueryExpansionConfig,
)
from databao.agent.executors.utils import (
    search_context as _search_context, _get_ds_name, _search_result_to_dict,
)
from databao.agent.executors.utils import (
    search_context_with_query_expansion as _search_context_with_query_expansion,
)


def make_search_context_tool(
    domain: Domain,
    *,
    make_structured: bool = False,
    expansion_llm: BaseChatModel | None = None,
    expansion_config: QueryExpansionConfig | None = None,
) -> BaseTool | None:
    if not domain.supports_context:
        return None
    if isinstance(domain, _DCEProjectDomain):
        return _make_dce_search_context_tool(domain, expansion_llm=expansion_llm, expansion_config=expansion_config, make_structured=make_structured)
    raise ValueError(f"Search context tool is not supported for domain type: {type(domain)}")


def _make_dce_search_context_tool(
    domain: _DCEProjectDomain,
    *,
    make_structured: bool = False,
    expansion_llm: BaseChatModel | None = None,
    expansion_config: QueryExpansionConfig | None = None,
) -> BaseTool | None:
    if expansion_llm is not None and expansion_config is not None:
        return _make_dce_expanded_search_tool(domain, expansion_llm, expansion_config)
    elif make_structured:
        return _make_dce_structured_search_tool(domain)
    return _make_dce_plain_search_tool(domain)

def extract_content(result_list, object_type, content_types):
    final_result = []
    for result in result_list:
        result_dict = yaml.safe_load(result.context_result)
        parsed_result = {}
        for content_type in content_types:
            if content_type not in result_dict:
                 parsed_result[content_type]=result_dict[object_type][content_type]
            else:
                parsed_result[content_type] = result_dict[content_type]
        final_result.append({
        "data_source_name": _get_ds_name(result),
        "score": result.score,
        "context_result": parsed_result,
    }
)
    return final_result

def _make_dce_structured_search_tool(domain: _DCEProjectDomain) -> BaseTool:
    """Build the search_context tool without query expansion."""

    @tool(parse_docstring=True)
    def search_context(query: str, object_type: Literal["table", "column"] | None, content_types: list[Literal["description", "samples", "stats", "name", "type"]] | None) -> list[dict[str, Any]]:
        """Search the context for relevant information matching the given query text.

        Use this tool to find additional information about the database (e.g., table and column descriptions and
        sample rows, column statistics (stats) or only the names of the objects) and any attached
        data sources (e.g., dbt projects).

        Prefer using this tool to get detailed database schema insights as opposed to running
        your own database inspection SQL queries.

        Your natural language query will be matched against a semantic and keyword based search index
        to find relevant results. Include specific information in the query (e.g., table names, column names)
        to get the best results.

        As returning all matching contents may include duplicated information - e.g. you asked for column names, but
        also tables are returned, you should preferably also specify the type of object you are looking for - e.g.
        table or column and also the type of content - e.g. you may alrady know the name of the columns you are
        interested in, but want to know their contents.

        If you specify `object_type` "table" you can only choose `content_types` "name" as for bigger tables returning
        the full information for possibly multiple table matches will easily bloat the context.
        To get fine-grained information you need to choose `object_type` "column".

        Args:
            query: Natural language query to search the context for relevant results.
            object_type (Literal["table", "column"] | None): The type of chunk to retrieve (e.g., table or column).)
            content_types (list[Literal["description", "samples", "stats", "name"]] | None): The types of content to retrieve.
        """
        search_result_list = domain.search_context(query, datasource_name=None, chunk_type=object_type)
        if content_types is None:
            return list(map(_search_result_to_dict, search_result_list))
        if object_type == "table":
            content_types = [content_type for content_type in content_types if content_type in {"stats", "name"}]
        parsed_result = extract_content(search_result_list, object_type, list(set(content_types+["name"])))
        return parsed_result

    return search_context



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
    query: Natural language query to search the context for relevant results.
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
