import re
from typing import Any, TextIO

import pandas as pd
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, BaseMessageChunk, ToolMessage

from portus.agents.frontend.messages import get_reasoning_content, get_tool_call_sql


class TextWriterFrontend:
    def __init__(self, *, writer: TextIO | None = None):
        self._writer = writer  # Use io.Writer type in Python 3.14
        self._is_tool_calling = False

    def write(self, text: str) -> None:
        print(text, end="", flush=True, file=self._writer)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        self.write(df.to_markdown())

    def add_message_chunk(self, chunk: BaseMessageChunk) -> None:
        if not isinstance(chunk, AIMessageChunk):
            return  # Handle ToolMessages in chunk.tag == StreamTag.messages branch

        reasoning_text = get_reasoning_content(chunk)
        text = reasoning_text + chunk.text()
        text = escape_text(text)
        self.write(text)

        if len(chunk.tool_call_chunks) > 0:
            # N.B. LangChain (currently) waits for the whole string to complete before yielding chunks
            # That's why long "sql" tool calls take some time to show up and then the whole sql is shown in a batch
            if not self._is_tool_calling:
                self.write("\n```\n")
                self._is_tool_calling = True
            for tool_call_chunk in chunk.tool_call_chunks:
                if tool_call_chunk["args"] is not None:
                    self.write(tool_call_chunk["args"])
        elif self._is_tool_calling:
            self.write("\n```\n")
            self._is_tool_calling = False

    def add_state_chunk(self, chunk: Any) -> None:
        if self._is_tool_calling:
            self.write("\n```\n")
            self._is_tool_calling = False

        messages: list[BaseMessage] = chunk.get("messages", [])
        for message in messages:
            if isinstance(message, ToolMessage):
                if message.artifact is not None:
                    if "df" in message.artifact and message.artifact["df"] is not None:
                        self.write_dataframe(message.artifact["df"])
                    else:
                        self.write(f"\n```\n{message.content}\n```\n")  # e.g., for errors
                else:
                    self.write(f"\n```\n{message.content}\n```\n")  # e.g., for errors
            elif isinstance(message, AIMessage):
                # During tool calling we show raw JSON chunks, but for SQL we also want pretty formatting.
                for tool_call in message.tool_calls:
                    sql = get_tool_call_sql(tool_call)
                    if sql is not None:
                        self.write(f"\n```sql\n{sql.sql}\n```\n")

    def add_chunk(self, mode: str, chunk: Any) -> None:
        if mode == "messages":
            token_chunk, _token_metadata = chunk
            self.add_message_chunk(token_chunk)
        elif mode == "values":
            self.add_state_chunk(chunk)


def escape_currency_dollar_signs(text: str) -> str:
    """Escapes dollar signs in a string to prevent MathJax interpretation in Streamlit."""
    return re.sub(r"\$(\d+)", r"\$\1", text)


def escape_strikethrough(text: str) -> str:
    return re.sub(r"~(.?\d+)", r"\~\1", text)


def escape_text(text: str) -> str:
    text = escape_strikethrough(text)
    text = escape_currency_dollar_signs(text)
    return text
