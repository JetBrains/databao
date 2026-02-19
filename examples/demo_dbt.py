import logging
import tempfile
from pathlib import Path

import duckdb

import databao
from databao import LLMConfig
from databao.configs.agent import AgentConfig
from databao.core.domain import _PersistentDomain
from databao.executors.dbt import DbtConfig, DbtProjectExecutor

logging.basicConfig(level=logging.INFO)

EXAMPLES_DIR = Path(__file__).resolve().parent
DBT_PROJ_PATH = EXAMPLES_DIR / "shopify002"
DB_PATH = DBT_PROJ_PATH / "shopify.duckdb"

llm_config = LLMConfig(name="gpt-5", temperature=0)
agent_config = AgentConfig(recursion_limit=100, parallel_tool_calls=True)

with tempfile.TemporaryDirectory() as tmp_dir:
    domain = _PersistentDomain(project_dir=Path(tmp_dir))
    engine = duckdb.connect(str(DB_PATH))
    domain.add_source(source=engine, name="shopify")

    agent = databao.agent(
        domain=domain,
        name="demo-dbt-executor",
        llm_config=llm_config,
        agent_config=agent_config,
        data_executor=DbtProjectExecutor(
            dbt_config=DbtConfig(
                project_dir=DBT_PROJ_PATH,
            ),
        ),
    )

    thread = agent.thread(stream_ask=True)

    thread.ask("What is our refund rate by month?")

    print("\n=== TEXT ===\n")
    print(thread.text())

    print("\n=== CODE ===\n")
    print(thread.code())

    print("\n=== Dataframe ===\n")
    print(thread.df())
