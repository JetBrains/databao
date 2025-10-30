from typing import NamedTuple

from langchain_core.messages import AIMessage, AIMessageChunk, ToolCall


class ToolCallSQL(NamedTuple):
    args_key: str
    sql: str


def get_tool_call_sql(tool_call: ToolCall) -> ToolCallSQL | None:
    args = tool_call["args"]
    # Currently, there is only run_sql_query with an sql param
    if "sql" in args:
        return ToolCallSQL("sql", args["sql"])
    return None


def get_reasoning_content(message: AIMessage | AIMessageChunk) -> str:
    # Assume only one of the reasoning parts is present, so there will be no duplication.
    reasoning_text = ""

    # OpenAI output_version: v0
    reasoning_chunk = message.additional_kwargs.get("reasoning", {})
    reasoning_summary_chunks = reasoning_chunk.get("summary", [])
    for reasoning_summary_chunk in reasoning_summary_chunks:
        reasoning_text += reasoning_summary_chunk.get("text", "")

    # "Qwen" style reasoning:
    reasoning_text += message.additional_kwargs.get("reasoning_content", "")

    # OpenAI output_version: responses/v1
    blocks = message.content if isinstance(message.content, list) else [message.content]
    for block in blocks:
        if isinstance(block, dict) and block.get("type", "text") == "reasoning":
            for summary in block["summary"]:
                reasoning_text += summary["text"]

    assert isinstance(reasoning_text, str), f"Expected a string, got {type(reasoning_text)}"
    return reasoning_text
