from pathlib import Path

from pandas import DataFrame
from sqlalchemy import Engine

from portus import LighthouseAgent
from portus.data_source.duckdb.duckdb_collection import DuckDBCollection
from portus.data_source.duckdb.duckdb_source import DuckDBSource
from portus.langchain_graphs.execute_submit import ExecuteSubmit
from portus.llms import LLMConfig
from portus.pipe.base_pipe import BasePipe
from portus.pipe.pipe import Pipe
from portus.session.base_session import BaseSession
from portus.vizualizer import DumbVisualizer, Visualizer

DEFAULT_TEMPLATE_PATH = Path("agent_system.jinja")


class InMemSession(BaseSession):
    def __init__(
        self,
        name: str,
        llm_config: LLMConfig,  # TODO parametrize using an agent, not an llm
        *,
        visualizer: Visualizer | None = None,
        default_rows_limit: int = 1000,
    ) -> None:
        self._name = name
        self._llm_config = llm_config

        self._visualizer = DumbVisualizer() if visualizer is None else visualizer
        self._default_rows_limit = default_rows_limit

        self._data_collection = DuckDBCollection()

    def add_db(self, engine: Engine, *, name: str | None = None) -> None:
        self._data_collection.add_db(engine, name)

    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        self._data_collection.add_df(df, name)

    def ask(self, query: str) -> BasePipe:
        self._data_collection.commit()
        agent = LighthouseAgent(
            self._data_collection,
            ExecuteSubmit(self._data_collection),
            self._llm_config,
            DEFAULT_TEMPLATE_PATH,
        )
        # agent = ReactDuckDBAgent(self._data_collection, get_chat_model(self._llm_config))
        return Pipe(agent, self._visualizer).ask(query)

    @property
    def sources(self) -> list[DuckDBSource]:
        return self._data_collection.sources

    @property
    def name(self) -> str:
        return self._name

    @property
    def visualizer(self) -> Visualizer:
        return self._visualizer
