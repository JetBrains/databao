from pathlib import Path
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from portus.agents.base import AgentExecutor
from portus.agents.lighthouse.graph import LighthouseAgentGraph
from portus.agents.lighthouse.utils import get_today_date_str, read_prompt_template
from portus.core import ExecutionResult, Opa, Session
from portus.data.configs.schema_inspection_config import SchemaInspectionConfig, SchemaSummaryType


class LighthouseAgent(AgentExecutor):
    def __init__(self, inspection_config: SchemaInspectionConfig | None = None) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__()
        self._inspection_config = inspection_config or SchemaInspectionConfig()

    def _get_agent_graph(self, session: Session) -> LighthouseAgentGraph:
        return LighthouseAgentGraph(
            session.data_engine,
            self._inspection_config,
            enable_inspect_tables_tool=self._inspection_config.summary_type == SchemaSummaryType.LIST_ALL_TABLES,
        )

    def render_system_prompt(self, session: Session) -> str:
        """Render system prompt with database schema."""
        data_engine = session.data_engine
        db_schema_str = data_engine.get_source_schemas_summarization_sync(self._inspection_config)
        # db_schema_str = asyncio.run(data_engine.get_source_schemas_summarization(self._inspection_config))  # Faster

        context = ""
        for db_name, db_context in session.db_contexts.items():
            if db_context is not None:
                context += f"## Context for {db_name}\n\n{db_context}\n\n"
        for df_name, df_context in session.df_contexts.items():
            if df_context is not None:
                context += f"## Context for {df_name}\n\n{df_context}\n\n"

        prompt_template = read_prompt_template(Path("system_prompt.jinja"))
        prompt = prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema_str,
            context=context,
        )
        return prompt

    def _create_graph(self, session: Session) -> CompiledStateGraph[Any]:
        """Create and compile the Lighthouse agent graph."""
        agent_graph = self._get_agent_graph(session)
        return agent_graph.compile(session.llm_config)

    def execute(
        self, session: Session, opa: Opa, *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        agent_graph = self._get_agent_graph(session)
        compiled_graph = self._get_or_create_cached_graph(session)

        messages = self._process_opa(session, opa, cache_scope)

        # Prepend system message if not present
        messages_with_system = messages
        if not messages_with_system or messages_with_system[0].type != "system":
            messages_with_system = [
                SystemMessage(self.render_system_prompt(session)),
                *messages_with_system,
            ]

        init_state = agent_graph.init_state(messages_with_system)
        last_state: dict[str, Any] | None = None
        try:
            for chunk in compiled_graph.stream(
                init_state,
                stream_mode="values",
                config=RunnableConfig(recursion_limit=50),
            ):
                assert isinstance(chunk, dict)
                last_state = chunk
        except Exception as e:
            return ExecutionResult(text=str(e), meta={"messages": messages_with_system})
        assert last_state is not None

        # Update message history (excluding system message which we add dynamically)
        final_messages = last_state.get("messages", [])
        if final_messages:
            messages_without_system = [msg for msg in final_messages if msg.type != "system"]
            self._update_message_history(session, cache_scope, messages_without_system)

        return agent_graph.get_result(last_state)
