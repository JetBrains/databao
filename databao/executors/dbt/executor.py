from __future__ import annotations

from typing import Any, TextIO

import duckdb
import jinja2
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph
from sqlalchemy import Connection, Engine

from databao.configs import LLMConfig
from databao.core import Cache, ExecutionResult, Opa
from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.core.executor import OutputModalityHints
from databao.dbt.config import DbtConfig
from databao.duckdb.types import DbConnFactory
from databao.duckdb.utils import get_db_path, register_sqlalchemy
from databao.executors.base import GraphExecutor
from databao.executors.dbt.graph import DbtProjectGraph
from databao.executors.lighthouse.history_cleaning import clean_tool_history


class DbtProjectExecutor(GraphExecutor):
    """
    A Lighthouse-style executor that runs the dbt project graph (DbtProjectGraph),
    but uses the *same* dbt system prompt rendering approach as the dbt LangChain agent:
    - assemble_dbt_project_summary(project_dir)
    - render databao.dbt/system_prompt.jinja with dbt_overview + dbt_directory
    """

    _DBT_TASK_INSTRUCTION = """\
    ## Your Objectives (in priority order):

    ### 1. FIX the dbt project if broken
    Before doing anything else, ensure the dbt project builds successfully:
    - Run `run_dbt` to check current state
    - If there are errors, diagnose and fix them (missing refs, syntax errors, schema mismatches)
    - Do NOT proceed to answer the user's question until `run_dbt` returns 0 errors

    ### 2. ANSWER the user's question
    Use `run_sql` to explore data and compute the answer.

    ### 3. CAPTURE reusable work as dbt models
    After answering, evaluate whether your work should be persisted:

    **CREATE a new model when:**
    - You calculated a metric the user or others might need again (e.g., "repeat purchase rate", "customer LTV")
    - You built a useful intermediate transformation (e.g., sessionized events, order-customer joins)
    - The logic encodes business rules that shouldn't be reimplemented

    **SKIP model creation when:**
    - The query is a one-off exploration (e.g., "show me 5 rows from X")
    - The transformation already exists in the project
    - The metric is trivial and unlikely to be reused

    **When creating models:**
    - Place them in the appropriate folder (`staging/`, `intermediate/`, `marts/`)
    - Use proper naming conventions (`stg_`, `int_`, `fct_`, `dim_`)
    - Add a brief description comment at the top
    - Run `run_dbt` to verify the new model builds
    - Confirm the model produces correct results with `run_sql`

    ### 4. VALIDATE before completing
    Your response is not complete until:
    - [ ] `run_dbt` passes with 0 errors
    - [ ] Your answer to the user is verified with actual query results
    - [ ] Any new models you created are building correctly
    """

    def __init__(self, *, dbt_config: DbtConfig, use_sandbox: bool = True) -> None:
        super().__init__()
        self._dbt_config = dbt_config
        self._use_sandbox = use_sandbox

        self._prompt_template = self._read_prompt_template("system_prompt.jinja")

        self._attached_db_paths: dict[str, str] = {}
        self._registered_dfs: dict[str, Any] = {}
        self._db_conn_factory: DbConnFactory | None = None
        self._graph = DbtProjectGraph(db_conn_factory=self._get_connection)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        for name, path in self._attached_db_paths.items():
            con.execute(f"ATTACH '{path}' AS {name} (READ_ONLY)")
        for name, df in self._registered_dfs.items():
            con.register(name, df)
        return con

    @staticmethod
    def _read_prompt_template(template_name: str) -> jinja2.Template:
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("databao.executors.dbt", ""),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return env.get_template(template_name)

    def render_system_prompt(self) -> str:
        from databao.dbt.agent import assemble_dbt_project_summary

        project_dir = self._dbt_config.project_dir.resolve()
        dbt_overview = assemble_dbt_project_summary(project_dir)

        system_prompt = self._prompt_template.render(
            dbt_overview=dbt_overview,
            dbt_directory=project_dir.absolute(),
        )
        return system_prompt.strip()

    def register_db(self, source: DBDataSource) -> None:
        connection = source.db_connection
        if isinstance(connection, Connection):
            connection = connection.engine

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is None:
                raise RuntimeError("Memory-based DuckDB is not supported.")
            connection.close()
            self._attached_db_paths[source.name] = path
            return

        if isinstance(connection, Engine):
            raise NotImplementedError("SQLAlchemy connections require a persistent connection; not yet supported with factory pattern.")

        raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

    def register_df(self, source: DFDataSource) -> None:
        self._registered_dfs[source.name] = source.df

    def _get_compiled_graph(self, llm_config: LLMConfig) -> CompiledStateGraph[Any]:
        compiled_graph = self._compiled_graph or self._graph.compile(llm_config)
        self._compiled_graph = compiled_graph
        return compiled_graph

    def drop_last_opa_group(self, cache: Cache, n: int = 1) -> None:
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
        sources: Sources,
        *,
        rows_limit: int = 100,
        stream: bool = True,
        writer: TextIO | None = None,
    ) -> ExecutionResult:
        compiled_graph = self._get_compiled_graph(llm_config)
        messages: list[BaseMessage] = self._process_opas(opas, cache)

        all_messages_with_system = messages
        if not all_messages_with_system or all_messages_with_system[0].type != "system":
            all_messages_with_system = [
                SystemMessage(self.render_system_prompt()),
                HumanMessage(self._DBT_TASK_INSTRUCTION),
                *all_messages_with_system,
            ]

        cleaned_messages = clean_tool_history(all_messages_with_system, llm_config.max_tokens_before_cleaning)

        project_dir = self._dbt_config.project_dir.resolve()

        if self._use_sandbox:
            init_state = self._graph.init_state_sandboxed(
                cleaned_messages,
                project_dir=project_dir,
                dbt_timeout_seconds=self._dbt_config.dbt_timeout_seconds,
            )
        else:
            pre_existing_files = [str(p.resolve()) for p in project_dir.rglob("*") if p.is_file()]
            init_state = self._graph.init_state(
                cleaned_messages,
                project_dir=project_dir,
                pre_existing_files=pre_existing_files,
                dbt_timeout_seconds=self._dbt_config.dbt_timeout_seconds,
            )

        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit or llm_config.agent_recursion_limit)
        last_state = self._invoke_graph_sync(
            compiled_graph, init_state, config=invoke_config, stream=stream, writer=writer or self._writer
        )

        result = self._graph.get_result(last_state)

        sandbox_result = None
        if self._use_sandbox:
            if self._graph.should_commit_sandbox(last_state):
                sandbox_result = self._graph.commit_sandbox()
                sandbox_result["committed"] = True
            else:
                self._graph.discard_sandbox()
                sandbox_result = {
                    "committed": False,
                    "reason": f"Last dbt run failed (returncode={last_state.get('last_dbt_returncode')})",
                }

        final_messages = last_state.get("messages", [])
        if final_messages:
            new_messages = final_messages[len(cleaned_messages):]
            all_messages = all_messages_with_system + new_messages
            all_messages_without_system = [m for m in all_messages if m.type != "system"]
            self._update_message_history(cache, all_messages_without_system)

        execution_result = ExecutionResult(
            text=str(result.get("text", "")),
            df=result.get("df"),
            code=result.get("code"),
            meta={
                "messages": final_messages or [],
                "tool_calls": result.get("tool_calls", []),
                "dbt_project_dir": str(project_dir),
                "sandbox_result": sandbox_result,
            },
        )

        execution_result.meta[OutputModalityHints.META_KEY] = OutputModalityHints(
            visualization_prompt=None,
            should_visualize=False,
        )
        return execution_result
