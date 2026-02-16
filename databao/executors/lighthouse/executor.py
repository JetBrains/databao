from pathlib import Path
from typing import Any, TextIO, cast

import duckdb
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core import Cache, Domain, ExecutionResult, Opa
from databao.core.data_source import DBDataSource, DFDataSource
from databao.core.domain import _Domain
from databao.core.executor import OutputModalityHints
from databao.databases import register_in_duckdb
from databao.duckdb.utils import describe_duckdb_schema
from databao.executors.base import GraphExecutor
from databao.executors.lighthouse.graph import ExecuteSubmit
from databao.executors.lighthouse.history_cleaning import clean_tool_history
from databao.executors.lighthouse.utils import get_today_date_str, read_prompt_template


class LighthouseExecutor(GraphExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._prompt_template = read_prompt_template(Path("system_prompt.jinja"))

        # Create a DuckDB connection for the agent
        self._duckdb_connection = duckdb.connect(":memory:")
        self._graph: ExecuteSubmit = ExecuteSubmit(self._duckdb_connection)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def render_system_prompt(
        self,
        data_connection: Any,
        domain: Domain,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        db_schema = describe_duckdb_schema(data_connection)

        domain = cast(_Domain, domain)
        sources = domain.sources
        context_text = ""
        for db_name, source in sources.dbs.items():
            if source.context:
                context_text += f"## Context for DB {db_name}\n\n{source.context}\n\n"
        for df_name, source in sources.dfs.items():
            if source.context:
                context_text += (
                    f"## Context for DF {df_name} (fully qualified name 'temp.main.{df_name}')\n\n{source.context}\n\n"
                )
        for idx, add_ctx in enumerate(sources.additional_context, start=1):
            context_text += f"## General information {idx}\n\n{add_ctx.strip()}\n\n"
        context_text = context_text.strip()

        prompt = self._prompt_template.render(
            date=get_today_date_str(), db_schema=db_schema, context=context_text, tool_limit=recursion_limit // 2
        )

        return prompt.strip()

    def register_db(self, source: DBDataSource) -> None:
        """Register DB in the DuckDB connection."""
        register_in_duckdb(self._duckdb_connection, source.config, source.name)

    def register_df(self, source: DFDataSource) -> None:
        self._duckdb_connection.register(source.name, source.df)

    def _get_compiled_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain
    ) -> CompiledStateGraph[Any]:
        """Get compiled graph."""
        compiled_graph = self._compiled_graph or self._graph.compile(llm_config, agent_config, domain)
        self._compiled_graph = compiled_graph

        return compiled_graph

    def drop_last_opa_group(self, cache: Cache, n: int = 1) -> None:
        """Drop last n groups of operations from the message history."""
        messages = cache.get("state", default={}).get("messages", [])
        human_messages = [m for m in messages if isinstance(m, HumanMessage)]
        if len(human_messages) < n:
            raise ValueError(f"Cannot drop last {n} operations - only {len(human_messages)} operations found.")
        c = 0
        while c < n:
            m = messages.pop()
            if isinstance(m, HumanMessage):
                c += 1

    def execute(
        self,
        opas: list[Opa],
        cache: Cache,
        llm_config: LLMConfig,
        agent_config: AgentConfig,
        domain: Domain,
        *,
        rows_limit: int = 100,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        compiled_graph = self._get_compiled_graph(llm_config, agent_config, domain)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        # Prepend system message if not present
        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt(self._duckdb_connection, domain, agent_config.recursion_limit)),
                *all_messages_with_system,
            ]
        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

        init_state = self._graph.init_state(cleaned_messages, limit_max_rows=rows_limit)
        invoke_config = RunnableConfig(recursion_limit=agent_config.recursion_limit)
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer
        )
        execution_result = self._graph.get_result(last_state)

        # Update message history (excluding system message which we add dynamically)
        final_messages = last_state.get("messages", [])
        if final_messages:
            new_messages = final_messages[len(cleaned_messages) :]
            all_messages = all_messages_with_system + new_messages
            all_messages_without_system = [msg for msg in all_messages if msg.type != "system"]
            if execution_result.meta.get(ExecutionResult.META_MESSAGES_KEY):
                execution_result.meta[ExecutionResult.META_MESSAGES_KEY] = all_messages
            self._update_message_history(cache, all_messages_without_system)

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
