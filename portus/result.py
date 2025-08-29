import abc
from abc import ABC
from langchain_core.language_models.chat_models import BaseChatModel
from portus.sql_gen import OneShotSqlGen
import pandas as pd


class Result(ABC):
    @abc.abstractmethod
    def df(self):
        pass

    @abc.abstractmethod
    def plot(self):
        pass


class LazyResult(Result):
    def __init__(
            self,
            llm: BaseChatModel,
            dbs: set[object],
            query: str
    ):
        self.__llm = llm
        self.__dbs = list(dbs)
        self.__query = query

        self.__materialized = False
        self.__text = None
        self.__df = None

    def __materialize(self):
        if not self.__materialized:
            # noinspection PyTypeChecker
            sql = OneShotSqlGen().gen(self.__query, self.__dbs[0], self.__llm)
            self.__text = sql
            self.__df = pd.read_sql(sql, self.__dbs[0])
            self.__materialized = True

    def df(self):
        self.__materialize()
        return self.__df

    def plot(self):
        pass

    def __str__(self):
        self.__materialize()
        return self.__text
