from typing import Any

from claude_agent_sdk.types import AssistantMessage as ClaudeAssistantMessage
from claude_agent_sdk.types import Message as ClaudeMessage
from claude_agent_sdk.types import ResultMessage as ClaudeResultMessage
from claude_agent_sdk.types import SystemMessage as ClaudeSystemMessage
from claude_agent_sdk.types import ThinkingBlock
from claude_agent_sdk.types import ToolResultBlock as ClaudeToolResultBlock
from claude_agent_sdk.types import ToolUseBlock as ClaudeToolUseBlock
from claude_agent_sdk.types import UserMessage as ClaudeUserMessage
from langchain_core.messages import AIMessage, BaseMessage, ChatMessage, SystemMessage, ToolCall, ToolMessage


def _separate_content_and_tool_calls(message: ClaudeAssistantMessage) -> tuple[list[ToolCall], list[str]]:
    tool_calls = []
    contents = []
    for block in message.content:
        if isinstance(block, ClaudeToolUseBlock):
            tool_calls.append(tool_call(block))
        elif isinstance(block, ThinkingBlock):
            contents.append(text_message(block.thinking))
        elif isinstance(block, ClaudeToolResultBlock):
            contents.append(text_message(str(block.content)))
        else:
            contents.append(text_message(block.text))
    return tool_calls, contents


def cast_claude_message_to_langchain_message(message: ClaudeMessage) -> BaseMessage:
    """
    Cast the ClaudeMessage (which is a Union and not a base class and thus cannot be
    uniformly processed further) into the child class of langchain.messages.BaseMessage
    which has the closest functionality - e.g. a UserMessage with tool_result content
    should be parsed as a ToolMessage.

    Args:
        message (ClaudeMessage): The raw output from claude sdk to be cast.
    """
    if isinstance(message, ClaudeAssistantMessage):
        tool_calls, contents = _separate_content_and_tool_calls(message)
        return AIMessage(content=contents, tool_calls=tool_calls)  # type: ignore [arg-type]

    if isinstance(message, ClaudeUserMessage):
        tool_result_blocks = [content for content in message.content if isinstance(content, ClaudeToolResultBlock)]
        non_empty_tool_results = [block for block in tool_result_blocks if block.content]
        if non_empty_tool_results:
            return ToolMessage(**tool_call_result(non_empty_tool_results))

        # This happens among other cases in subagent messages
        return ChatMessage(role="claude-user", content=str(message.content))

    if isinstance(message, ClaudeSystemMessage):
        return SystemMessage(content=[], **claude_message(message.data))

    if isinstance(message, ClaudeResultMessage):
        return AIMessage(content=text_message(message.result or ""))

    raise TypeError(f"Unknown message type: {type(message)}")


# define wrappers for more legible weave logs
def text_message(content: str) -> str:
    return content


def tool_call(block: ClaudeToolUseBlock) -> ToolCall:
    return ToolCall(name=block.name, args=block.input, id=block.id)


def tool_call_result(contents: list[ClaudeToolResultBlock]) -> dict[str, Any]:
    content: list[str | dict[str, Any]] = []
    for block in contents:
        if isinstance(block.content, str):
            content.append(block.content)
        elif isinstance(block.content, list):
            content.extend(block.content)
    return dict(tool_call_id=contents[0].tool_use_id, content=content)


def claude_message(input_: Any) -> Any:
    return input_
