import logging
from typing import Any, TextIO, cast

from claude_agent_sdk import SdkMcpTool
from duckdb import DuckDBPyConnection

from databao.agent.configs import LLMConfig
from databao.agent.configs.agent import AgentConfig
from databao.agent.core import Cache, Domain, ExecutionResult, Opa
from databao.agent.core.domain import _Domain
from databao.agent.databases.databases import db_type as get_db_type
from databao.agent.executors import LighthouseExecutor
from databao.agent.executors.base import DuckDBExecutor
from databao.agent.executors.claude_code.claude_model_wrapper import ClaudeModelWrapper
from databao.agent.executors.prompt import build_context_text, get_today_date_str, load_prompt_template

_LOGGER = logging.getLogger(__name__)


class ClaudeCodeExecutor(DuckDBExecutor[SdkMcpTool[Any]]):
    def __init__(self, writer: Any = None) -> None:
        super().__init__(writer=writer)
        self._prompt_template = load_prompt_template("databao.agent.executors.claude_code", "system_prompt.jinja")

        self._max_columns_per_table: int | None = None
        self._max_schema_summary_length: int | None = 250_000  # 1 token ~= 4 characters
        self._claude_session_id: str | None = None

    def register_tools(self, tools: list[SdkMcpTool[Any]]) -> None:
        """Register additional tools to be available during execution."""
        # TODO: add to allowed tool list

    def drop_last_opa_group(self, cache: "Cache", n: int = 1) -> None:
        pass

    def render_system_prompt(
        self,
        data_connection: DuckDBPyConnection,
        domain: Domain,
        recursion_limit: int = 50,
    ) -> str:
        """Render system prompt with database schema."""
        domain = cast(_Domain, domain)

        db_types = {}
        for name, source in domain.sources.dbs.items():
            db_type = get_db_type(source.config).full_type
            db_types[name] = db_type

        db_schema = LighthouseExecutor.inspect_database_schema(
            data_connection,
            db_types,
            max_schema_summary_length=self._max_schema_summary_length,
            max_columns_per_table=self._max_columns_per_table,
        )

        sources = domain.sources
        context_text = build_context_text(
            sources,
            df_label_fn=lambda name: f"DF {name} (fully qualified name 'temp.main.{name}')",
        )

        dce_search_enabled = False

        prompt = self._prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
            context=context_text,
            tool_limit=recursion_limit // 2,
            db_types=db_types,
            dce_search_enabled=dce_search_enabled,
        )

        return prompt.strip()

    def _process_opas(self, opas: list[Opa], cache: Cache) -> str:
        """
        Process a single opa and convert it to a message, appending to message history.

        Returns:
            All messages including the new one
        """
        messages: list[Any] = cache.get("state", {}).get("messages", [])
        query = "\n\n".join(opa.query for opa in opas)
        messages.append(query)
        return "\n".join(messages)

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
        self._init_sources_from_domain(domain)
        system_prompt = self.render_system_prompt(self._duckdb_connection, domain, agent_config.recursion_limit)
        with ClaudeModelWrapper(
            config=llm_config, connection=self._duckdb_connection, system_prompt=system_prompt, session_id=self._claude_session_id
        ) as agent:
            user_messages: str = self._process_opas(opas, cache)
            execution_result, self._claude_session_id = agent.ask(user_messages, stream=stream)



        return execution_result
