from pathlib import Path
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig

from databao.agents.base import AgentExecutor
from databao.agents.lighthouse.graph import ExecuteSubmit
from databao.agents.lighthouse.utils import get_today_date_str, read_prompt_template
from databao.configs.llm import LLMConfig
from databao.core import ExecutionResult, Opa, Session
from databao.duckdb.utils import describe_duckdb_schema


class LighthouseAgent(AgentExecutor):
    def __init__(self) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__()
        self._cached_graph: ExecuteSubmit | None = None
        self._prompt_template = read_prompt_template(Path("system_prompt.jinja"))

    def render_system_prompt(self, data_connection: Any, session: Session) -> str:
        """Render system prompt with database schema."""
        db_schema = describe_duckdb_schema(data_connection)

        db_contexts, df_contexts = session.context
        context = ""
        for db_name, db_context in db_contexts.items():
            context += f"## Context for DB {db_name}\n\n{db_context}\n\n"
        for df_name, df_context in df_contexts.items():
            context += f"## Context for DF {df_name} (fully qualified name 'temp.main.{df_name}')\n\n{df_context}\n\n"
        for idx, additional_ctx in enumerate(session.additional_context, start=1):
            additional_context = additional_ctx.strip()
            context += f"## Additional context {idx}\n\n{additional_context}\n\n"
        context = context.strip()

        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
            context=context,
        )

        return prompt

    def _create_graph(self, data_connection: Any, llm_config: LLMConfig) -> Any:
        """Create and compile the Lighthouse agent graph."""
        self._cached_graph = ExecuteSubmit(data_connection)
        return self._cached_graph.compile(llm_config)

    def _get_graph_and_compiled(self, session: Session) -> tuple[Any, ExecuteSubmit, Any]:
        """Get connection, uncompiled graph, and compiled graph."""
        data_connection, compiled_graph = self._get_or_create_cached_graph(session)

        if self._cached_graph is None:
            raise RuntimeError("Graph was not properly initialized after creation")

        return data_connection, self._cached_graph, compiled_graph

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
        data_connection, graph, compiled_graph = self._get_graph_and_compiled(session)

        messages = self._process_opa(session, opa, cache_scope)

        # Prepend system message if not present
        messages_with_system = messages
        if not messages_with_system or messages_with_system[0].type != "system":
            messages_with_system = [
                SystemMessage(self.render_system_prompt(data_connection, session)),
                *messages_with_system,
            ]

        init_state = graph.init_state(messages_with_system)
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph(compiled_graph, init_state, config=invoke_config, stream=stream)
        execution_result = graph.get_result(last_state)

        # Update message history (excluding system message which we add dynamically)
        final_messages = last_state.get("messages", [])
        if final_messages:
            messages_without_system = [msg for msg in final_messages if msg.type != "system"]
            self._update_message_history(session, cache_scope, messages_without_system)

        return execution_result
