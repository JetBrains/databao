import abc
from abc import ABC

from portus.result import Result, LazyResult
from langchain_core.language_models.chat_models import BaseChatModel


class Session(ABC):
    @abc.abstractmethod
    def add_db(self, engine: "Engine") -> None:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> Result:
        pass


class SessionImpl(Session):
    def __init__(self, llm: BaseChatModel):
        self.__dbs = set()
        self.__llm = llm

    def add_db(self, connection: "Engine") -> None:
        self.__dbs.add(connection)

    def ask(self, query: str) -> Result:
        return LazyResult(self.__llm, self.__dbs, query)
