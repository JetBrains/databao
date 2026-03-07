from collections.abc import Sequence
from typing import Any

from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI

from databao.agent.configs.llm import LLMConfig


def model_bind_tools(
    model: BaseChatModel, tools: Sequence[BaseTool], **kwargs: Any
) -> Runnable[LanguageModelInput, BaseMessage]:
    """Bind tools to a model, using strict mode for OpenAI models."""
    if isinstance(model, ChatOpenAI):
        return model.bind_tools(tools, strict=True, **kwargs)
    return model.bind_tools(tools, **kwargs)


def call_model_with_retry(model: Runnable[list[BaseMessage], Any], messages: list[BaseMessage]) -> Any:
    """Invoke the model with exponential jitter retry (up to 3 attempts)."""
    return model.with_retry(wait_exponential_jitter=True, stop_after_attempt=3).invoke(messages)


def is_anthropic_model(config: LLMConfig) -> bool:
    """Check if the model is an Anthropic model based on the config name."""
    return "claude" in config.name.lower()


def chat(
    messages: list[BaseMessage],
    config: LLMConfig,
    model: Runnable[list[BaseMessage], Any] | None = None,
) -> list[BaseMessage]:
    """Run a single chat turn: apply caching, invoke model, return messages + response."""
    if model is None:
        model = config.new_chat_model()
    messages = apply_system_prompt_caching(config, messages)
    response: AIMessage = call_model_with_retry(model, messages)
    return [*messages, response]


def apply_system_prompt_caching(config: LLMConfig, messages: list[BaseMessage]) -> list[BaseMessage]:
    """Apply system prompt caching for Anthropic models."""
    if not (config.cache_system_prompt and is_anthropic_model(config)):
        return messages
    assert all(m.type != "system" for m in messages[1:])
    if messages[0].type == "system":
        messages = [_set_message_cache_breakpoint(config, messages[0]), *messages[1:]]
    return messages


def _set_message_cache_breakpoint(config: LLMConfig, message: BaseMessage) -> BaseMessage:
    if not is_anthropic_model(config):
        return message
    new_content: list[dict[str, Any] | str]
    match message.content:
        case str() | dict():
            new_content = [_set_anthropic_cache_breakpoint(message.content)]
        case list():
            new_content = message.content.copy()
            new_content[-1] = _set_anthropic_cache_breakpoint(new_content[-1])
    return message.model_copy(update={"content": new_content})


def _set_anthropic_cache_breakpoint(content: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(content, str):
        return {"type": "text", "text": content, "cache_control": {"type": "ephemeral"}}
    elif isinstance(content, dict):
        d = content.copy()
        d["cache_control"] = {"type": "ephemeral"}
        return d
    else:
        raise ValueError(f"Unknown content type: {type(content)}")
