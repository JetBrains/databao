from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage


@dataclass(frozen=True)
class QueryExpansionConfig:
    """Configuration for the query expansion step.

    :ivar num_queries: Number of expanded queries to generate.
    :ivar rrf_k: Rank fusion constant (standard default is 60).
    """

    num_queries: int = 3
    rrf_k: int = 60


_SYSTEM_PROMPT = """\
You are a retrieval query expansion assistant.
Your job is to take a user's natural language question and produce {num_queries} short, \
diverse search queries optimized for retrieving relevant schema objects from a **{datasource_type}** project.

Rules:
- Each query should be a concise phrase (3-8 words max).
- Adapt to the datasource naming conventions. For example, in dbt projects tables often look \
like "shopify__refund_rate", "stg_orders", "fct_monthly_revenue", so prefer that style over plain English.
- Include the original intent as one query, and vary the rest: synonyms, table-name guesses, \
metric names, related concepts.
- Return ONLY a JSON array of strings, nothing else.
"""


def expand_queries(
    query: str,
    llm: BaseChatModel,
    config: QueryExpansionConfig,
    datasource_type: str = "sql",
) -> list[str]:
    """Use an LLM to expand a single query into multiple retrieval-friendly queries."""
    messages = [
        SystemMessage(
            content=_SYSTEM_PROMPT.format(
                num_queries=config.num_queries,
                datasource_type=datasource_type,
            )
        ),
        HumanMessage(content=query),
    ]
    response = llm.invoke(messages)
    text = response.text.strip()

    # Robust JSON extraction — handle markdown fences
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    try:
        queries = json.loads(text)
        if isinstance(queries, list) and all(isinstance(q, str) for q in queries):
            return queries[: config.num_queries]
    except (json.JSONDecodeError, TypeError):
        pass

    return [query]


def reciprocal_rank_fusion(
    ranked_lists: list[list[dict[str, Any]]],
    *,
    k: int = 60,
    key: str = "context_result",
) -> list[dict[str, Any]]:
    """Merge multiple ranked result lists using Reciprocal Rank Fusion.

    Each result dict is identified by its ``key`` field for deduplication.
    The output is sorted by descending RRF score, with the ``distance`` field
    replaced by ``1 - normalized_rrf_score`` so lower-is-better semantics are preserved.
    """
    scores: dict[str, float] = {}
    best_result: dict[str, dict[str, Any]] = {}

    for ranked_list in ranked_lists:
        for rank, result in enumerate(ranked_list):
            identifier = str(result.get(key, id(result)))
            rrf_score = 1.0 / (k + rank + 1)
            scores[identifier] = scores.get(identifier, 0.0) + rrf_score
            if identifier not in best_result:
                best_result[identifier] = result

    if not scores:
        return []

    max_score = max(scores.values())
    fused: list[dict[str, Any]] = []
    for identifier, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        result = best_result[identifier].copy()
        result["distance"] = 1.0 - (score / max_score) if max_score > 0 else 1.0
        fused.append(result)

    return fused
