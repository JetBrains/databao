from langchain_core.messages import AIMessage, BaseMessage, ToolCall, ToolMessage
from langchain_core.messages.utils import count_tokens_approximately


def _truncate_no_df_block(messages: list[BaseMessage]) -> AIMessage:
    """Returns one AIMessage with only the last message."""
    assert messages[-1].type == "ai"
    text = f"""Message history was truncated. {len(messages) - 1} messages were deleted.

Here is an answer, which was shown to the user:
""" + str(messages[-1].content)
    return AIMessage(content=text)


def _truncate_block(dfs: dict[str, dict[str, str]], messages: list[BaseMessage]) -> AIMessage:
    """Returns one AIMessage with a summary of the block."""
    assert isinstance(messages[-1], ToolMessage)
    assert isinstance(messages[-2], AIMessage)
    df = None
    for d in dfs.values():
        if d.get("query_id") == messages[-2].tool_calls[0]["args"].get("query_id"):
            df = d
            break
    assert df is not None
    tool_call: ToolCall = messages[-2].tool_calls[0]
    text = f"""Message history was truncated. {len(messages) - 1} messages were deleted.

This SQL was generated:
```sql
{df["sql"]}
```

Here is an answer, which was shown to the user:
Dataframe:
{df["df"]}

Text:
{tool_call["args"]["result_description"]}
"""
    if tool_call["args"].get("visualization_prompt"):
        text += f"\n\nVisualization prompt: {tool_call['args']['visualization_prompt']}"
    print(f"Message history was truncated. {len(messages) - 1} messages were deleted.")
    return AIMessage(content=text)


def clean_tool_history(
    all_messages: list[BaseMessage], token_limit: int, current_messages: list[BaseMessage] | None = None
) -> list[BaseMessage]:
    """
    If message history exceeds token limit, truncates it.
    It removes all intermediate messages and changes a final AI message.
    The final message contains SQL, dataframe and text.

    Returns: messages ready to be sent to LLM.
    """
    current_messages = current_messages or all_messages
    if count_tokens_approximately(current_messages) < token_limit:
        print(f"Message history is within token limit. {count_tokens_approximately(current_messages)} < {token_limit}.")
        return current_messages

    dfs: dict[str, dict[str, str]] = {}
    buffer = []
    result: list[BaseMessage] = []
    for i in range(len(current_messages)):
        buffer.append(current_messages[i])
        curr_message = current_messages[i]
        if isinstance(curr_message, AIMessage):
            # Fill `dfs` dict
            if curr_message.tool_calls:
                for tool_call in curr_message.tool_calls:
                    if tool_call["name"] == "run_sql_query":
                        call_id = str(tool_call["id"])
                        sql = tool_call["args"]["sql"]
                        dfs[call_id] = {"sql": sql}
            else:
                if len(buffer) > 3:
                    # Long thread with no submission at the end.
                    result.append(_truncate_no_df_block(buffer))
                    buffer = []

        elif isinstance(curr_message, ToolMessage):
            call_id = curr_message.tool_call_id
            if call_id in dfs and curr_message.artifact is not None and "csv" in curr_message.artifact:
                # Enrich `dfs` dict with calculation results
                dfs[call_id]["df"] = curr_message.artifact.get("csv")
                dfs[call_id]["query_id"] = curr_message.artifact.get("query_id")
            elif current_messages[i - 1].tool_calls[0]["name"] == "submit_query_id":  # type: ignore
                result.append(_truncate_block(dfs, buffer))
                buffer = []

        else:
            # For system and human messages
            result.extend(buffer)
            buffer = []

    return result
