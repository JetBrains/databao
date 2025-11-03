import re
from typing import Any, TextIO

import pandas as pd
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, BaseMessageChunk, ToolMessage

from databao.agents.frontend.messages import get_reasoning_content, get_tool_call_sql


class TextStreamFrontend:
    """Helper for streaming LangGraph LLM outputs to a text stream (stdout, stderr, a file, etc.)."""

    def __init__(
        self,
        start_state: dict[str, Any],
        *,
        writer: TextIO | None = None,
        escape_markdown: bool = False,
        show_headers: bool = True,
        pretty_sql: bool = False,
    ):
        self._writer = writer  # Use io.Writer type in Python 3.14
        self._escape_markdown = escape_markdown
        self._show_headers = show_headers
        self._message_count = len(start_state.get("messages", []))
        self._started = False
        self._is_tool_calling = False
        self._pretty_sql = pretty_sql

    def write(self, text: str) -> None:
        if not self._started:
            self.start()
        print(text, end="", flush=True, file=self._writer)

    def write_dataframe(self, df: pd.DataFrame) -> None:
        self.write(df.to_markdown())

    def write_message_chunk(self, chunk: BaseMessageChunk) -> None:
        if not isinstance(chunk, AIMessageChunk):
            return  # Handle ToolMessage results in add_state_chunk

        reasoning_text = get_reasoning_content(chunk)
        text = reasoning_text + chunk.text()
        if self._escape_markdown:
            text = escape_markdown_text(text)
        self.write(text)

        if len(chunk.tool_call_chunks) > 0:
            # N.B. LangChain sometimes waits for the whole string to complete before yielding chunks
            # That's why long "sql" tool calls take some time to show up and then the whole sql is shown in a batch
            if not self._is_tool_calling:
                self.write("\n\n```\n")  # Open code block
                self._is_tool_calling = True
            for tool_call_chunk in chunk.tool_call_chunks:
                if tool_call_chunk["args"] is not None:
                    self.write(tool_call_chunk["args"])
        elif self._is_tool_calling:
            self.write("\n```\n\n")  # Close code block
            self._is_tool_calling = False

    def write_state_chunk(self, state_chunk: dict[str, Any]) -> None:
        """The state chunk is assumed to contain a "messages" key."""
        if self._is_tool_calling:
            self.write("\n```\n\n")  # Close code block
            self._is_tool_calling = False

        # Loop through new messages only.
        # We could either force the caller of the frontend to provide new messages only,
        # but for ease of use we assume the state contains a list of messages and do it here.
        messages: list[BaseMessage] = state_chunk.get("messages", [])
        new_messages = messages[self._message_count :]
        self._message_count += len(new_messages)

        for message in new_messages:
            if isinstance(message, ToolMessage):
                if message.artifact is not None:
                    if "df" in message.artifact and message.artifact["df"] is not None:
                        self.write_dataframe(message.artifact["df"])
                    else:
                        self.write(f"\n```\n{message.content}\n```\n\n")
                else:
                    self.write(f"\n```\n{message.content}\n```\n\n")
            elif self._pretty_sql and isinstance(message, AIMessage):
                # During tool calling we show raw JSON chunks, but for SQL we also want pretty formatting.
                for tool_call in message.tool_calls:
                    sql = get_tool_call_sql(tool_call)
                    if sql is not None:
                        self.write(f"\n```sql\n{sql}\n```\n\n")

    def write_stream_chunk(self, mode: str, chunk: Any) -> None:
        if mode == "messages":
            token_chunk, _token_metadata = chunk
            self.write_message_chunk(token_chunk)
        elif mode == "values":
            if isinstance(chunk, dict):
                self.write_state_chunk(chunk)
            else:
                raise ValueError(f"Unexpected chunk type: {type(chunk)}")

    def start(self) -> None:
        self._started = True
        if self._show_headers:
            self.write("=" * 8 + " <THINKING> " + "=" * 8 + "\n\n")

    def end(self) -> None:
        if self._show_headers:
            self.write("\n" + "=" * 8 + " </THINKING> " + "=" * 8 + "\n\n")
        self._started = False


def escape_currency_dollar_signs(text: str) -> str:
    """Escapes dollar signs in a string to prevent MathJax interpretation in markdown environments."""
    return re.sub(r"\$(\d+)", r"\$\1", text)


def escape_strikethrough(text: str) -> str:
    """Prevents aggressive markdown strikethrough formatting."""
    return re.sub(r"~(.?\d+)", r"\~\1", text)


def escape_markdown_text(text: str) -> str:
    text = escape_strikethrough(text)
    text = escape_currency_dollar_signs(text)
    return text
