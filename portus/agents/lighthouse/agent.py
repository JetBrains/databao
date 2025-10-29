from pathlib import Path
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from portus.agents.base import AgentExecutor
from portus.agents.lighthouse.graph import ExecuteSubmit
from portus.agents.lighthouse.utils import get_today_date_str, read_prompt_template
from portus.core import ExecutionResult, Opa, Session
from portus.data.configs.schema_inspection_config import SchemaInspectionConfig, SchemaSummaryType


class LighthouseAgent(AgentExecutor):
    def __init__(self) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__()
        self._inspection_config = SchemaInspectionConfig(summary_type=SchemaSummaryType.FULL)

    def render_system_prompt(self, session: Session) -> str:
        """Render system prompt with database schema."""
        data_engine = session.data_engine
        db_schema_str = data_engine.get_source_schemas_summarization_sync(self._inspection_config)
        # db_schema_str = asyncio.run(data_engine.get_source_schemas_summarization(self._inspection_config))  # Faster

        # TODO add "context" as the DatabaseSchema.description?
        db_contexts, df_contexts = session.context
        context = ""
        for db_name, db_context in db_contexts.items():
            context += f"## Context for DB {db_name}\n\n{db_context}\n\n"
        for df_name, df_context in df_contexts.items():
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
        agent_graph = ExecuteSubmit(session.data_engine)
        return agent_graph.compile(session.llm_config)

    def execute(
        self, session: Session, opa: Opa, *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        agent_graph = ExecuteSubmit(session.data_engine)
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
