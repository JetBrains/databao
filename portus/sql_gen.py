from abc import abstractmethod, ABC
from typing import Optional
from sqlalchemy import Engine
import logging
import re

from langchain_core.language_models.chat_models import BaseChatModel
from portus.meta_fetcher import DBEngineMetaFetcher

logger = logging.getLogger(__name__)


class SqlGen(ABC):
    @abstractmethod
    def gen(self,
            prompt: str,
            engine: Engine,
            llm: BaseChatModel,
            *,
            target_schema: Optional[str] = None,
            table_limit: int = 100,
            columns_per_table_limit: int = 50,
            max_rows: Optional[int] = 1000
            ) -> str:
        pass


class OneShotSqlGen(SqlGen):
    @staticmethod
    def __build_system_prompt(dialect: str, limit_rows: Optional[int]) -> str:
        limit_clause = f"Include 'LIMIT {limit_rows}' (or the dialect equivalent) unless a LIMIT/OFFSET is already present." if limit_rows else ""
        return (
            "You are a senior data analyst that writes correct, efficient SQL for the given database schema.\n"
            f"- Use SQL dialect: {dialect}.\n"
            "- Only output a single executable SQL query without explanations or markdown fences.\n"
            "- Prefer selecting explicit columns and include necessary JOIN conditions.\n"
            f"- {limit_clause}\n"
            "- If something is ambiguous, make reasonable assumptions."
        ).strip()

    @staticmethod
    def __strip_sql_fences(text: str) -> str:
        # Remove ```sql ... ``` or ``` ... ``` fences
        text = text.strip()
        fence_block = re.compile(r"^```(?:sql)?\s*([\s\S]*?)\s*```$", re.IGNORECASE)
        m = fence_block.match(text)
        if m:
            return m.group(1).strip()
        # Remove inline triple backticks if present
        text = re.sub(r"```", "", text).strip()
        # Quick fix for sqlalchemy params
        text = text.replace("%", "")
        # Also remove surrounding backticks if present
        return text.strip("` \n\r\t")

    def gen(self,
            prompt: str,
            engine: Engine,
            llm: BaseChatModel,
            *,
            target_schema: Optional[str] = None,
            table_limit: int = 100,
            columns_per_table_limit: int = 50,
            max_rows: Optional[int] = 1000
            ) -> str:
        fetcher = DBEngineMetaFetcher(engine)
        schema_text = fetcher.schema(table_limit=table_limit, cols_limit=columns_per_table_limit)
        dialect_name = fetcher.dialect()

        logger.info("Using dialect=%s", dialect_name)
        logger.debug("Fetched DB schema overview:\n%s", schema_text)

        try:
            # Support both message-based and string-based invocation
            system_prompt = self.__build_system_prompt(dialect_name, max_rows)
            user_prompt = (
                f"Prompt:\n{prompt}\n\n"
                f"Database schema overview:\n{schema_text}\n\n"
                "Return only the SQL query:"
            )
            try:
                result = llm.invoke(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                )
                content = getattr(result, "content", result)  # Some models return object with .content
                sql_query = self.__strip_sql_fences(content if isinstance(content, str) else str(content))
            except TypeError:
                # Fallback to simple string invocation
                combined = system_prompt + "\n\n" + user_prompt
                result = llm.invoke(combined)  # type: ignore
                content = getattr(result, "content", result)
                sql_query = self.__strip_sql_fences(content if isinstance(content, str) else str(content))
        except Exception as e:
            raise RuntimeError(f"LLM generation failed: {e}") from e

        if not isinstance(sql_query, str) or not sql_query.strip():
            raise RuntimeError("LLM did not return a valid SQL string.")

        logger.info("Generated query: %s", sql_query)

        return sql_query
