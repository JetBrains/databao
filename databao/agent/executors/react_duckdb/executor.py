import logging
from typing import Any, TextIO

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph.state import CompiledStateGraph

from databao.agent.configs.agent import AgentConfig
from databao.agent.configs.llm import LLMConfig
from databao.agent.core import Cache, Domain, ExecutionResult, Opa
from databao.agent.core.executor import OutputModalityHints
from databao.agent.duckdb.react_tools import AgentResponse, execute_duckdb_sql, make_react_duckdb_agent
from databao.agent.executors.base import GraphExecutor

logger = logging.getLogger(__name__)


class ReactDuckDBExecutor(GraphExecutor):
    def __init__(self, writer: Any = None) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__(writer=writer)

    def _compile_graph(
        self, llm_config: LLMConfig, agent_config: AgentConfig, domain: Domain, extra_tools: list[BaseTool] | None
    ) -> CompiledStateGraph[Any]:
        return make_react_duckdb_agent(
            self._duckdb_connection, llm_config.new_chat_model(), domain, extra_tools=extra_tools
        )

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
        compiled_graph = self._get_compiled_graph(llm_config, agent_config, domain)

        # Process the opa and get messages
        messages = self._process_opas(opas, cache)

        # Execute the graph
        init_state = {"messages": messages}
        invoke_config = RunnableConfig(recursion_limit=agent_config.recursion_limit)
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer
        )
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = execute_duckdb_sql(answer.sql, self._duckdb_connection, limit=rows_limit)

        # Update message history
        final_messages = last_state.get("messages", [])
        self._update_message_history(cache, final_messages)

        execution_result = ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
