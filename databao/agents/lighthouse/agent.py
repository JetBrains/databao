from pathlib import Path
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.agents.base import AgentExecutor
from databao.agents.lighthouse.graph import LighthouseAgentGraph
from databao.agents.lighthouse.utils import get_today_date_str, read_prompt_template
from databao.core import ExecutionResult, Opa, Session
from databao.data.configs.schema_inspection_config import SchemaInspectionConfig, SchemaSummaryType


class LighthouseAgent(AgentExecutor):
    def __init__(self, inspection_config: SchemaInspectionConfig | None = None) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__()
        self._inspection_config = inspection_config or SchemaInspectionConfig(summary_type=SchemaSummaryType.FULL)

    def _get_agent_graph(self) -> LighthouseAgentGraph:
        schema = self._get_or_inspect_schema(self._inspection_config.inspection_options)
        return LighthouseAgentGraph(
            self._duckdb_collection,
            schema,
            enable_inspect_tables_tool=self._inspection_config.summary_type == SchemaSummaryType.LIST_ALL_TABLES,
        )

    def render_system_prompt(self, session: Session) -> str:
        """Render system prompt with database schema."""
        db_schema_str = self._summarize_schema(self._inspection_config)

        # TODO add "context" as the DatabaseSchema.description?
        context = ""
        for db_name, db_context in session.db_contexts.items():
            context += f"## Context for DB {db_name}\n\n{db_context}\n\n"
        for df_name, df_context in session.df_contexts.items():
            context += f"## Context for DF {df_name}\n\n{df_context}\n\n"

        prompt_template = read_prompt_template(Path("system_prompt.jinja"))
        prompt = prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema_str,
            context=context,
        )
        return prompt

    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Create and compile the Lighthouse agent graph."""
        agent_graph = self._get_agent_graph()
        return agent_graph.compile(session.llm_config)

    def execute(
        self,
        session: Session,
        opa: Opa,
        *,
        rows_limit: int = 100,
        cache_scope: str = "common_cache",
        stream: bool = True,
    ) -> ExecutionResult:
        # TODO rows_limit is ignored

        # Get or create graph (cached after first use)
        compiled_graph = self._get_or_create_cached_graph(session)
        agent_graph = self._get_agent_graph()

        messages = self._process_opa(session, opa, cache_scope)

        # Prepend system message if not present
        messages_with_system = messages
        if not messages_with_system or messages_with_system[0].type != "system":
            messages_with_system = [
                SystemMessage(self.render_system_prompt(session)),
                *messages_with_system,
            ]

        init_state = agent_graph.init_state(messages_with_system)
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph(compiled_graph, init_state, config=invoke_config, stream=stream)
        execution_result = agent_graph.get_result(last_state)

        # Update message history (excluding system message which we add dynamically)
        final_messages = last_state.get("messages", [])
        if final_messages:
            messages_without_system = [msg for msg in final_messages if msg.type != "system"]
            self._update_message_history(session, cache_scope, messages_without_system)

        return execution_result
