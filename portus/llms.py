import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, overload

from langchain.chat_models import init_chat_model
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel, LanguageModelInput
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI

OPENAI_INFIXES = ["gpt", "openai", "o1", "o3"]
ANTHROPIC_INFIXES = ["claude", "anthropic"]
GEMINI_INFIXES = ["gemini", "google"]
REASONING_MODEL_PREFIXES = ["o1", "o3", "gpt-5"]


@dataclass(kw_only=True)
class LLMConfig:
    name: str
    temperature: float = 0.0
    max_tokens: int = 8192
    reasoning_effort: str = "medium"
    """Reasoning effort is used for OpenAI reasoning models only. 
    Warning: reasoning can use a lot of tokens! OpenAI recommends at least 25000 tokens"""
    seed: int = 7
    cache_system_prompt: bool = True
    """Cache system prompt with prompt caching. Only used for Anthropic models."""
    model_kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Additional kwargs for the model constructor."""

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def is_reasoning_model(model_name: str) -> bool:
    """Check if a model is a reasoning model based on its name."""
    return any(prefix in model_name for prefix in REASONING_MODEL_PREFIXES)


def is_openai_model(model_name: str) -> bool:
    """Check if a model is an OpenAI model based on its name."""
    return any(prefix in model_name for prefix in OPENAI_INFIXES)


def is_anthropic_model(model_name: str) -> bool:
    """Check if a model is an Anthropic model based on its name."""
    return any(prefix in model_name for prefix in ANTHROPIC_INFIXES)


def is_gemini_model(model_name: str) -> bool:
    """Check if a model is a Gemini model based on its name."""
    return any(prefix in model_name for prefix in GEMINI_INFIXES)


@overload
def get_chat_model(config_or_name: str) -> BaseChatModel: ...
@overload
def get_chat_model(config_or_name: LLMConfig) -> BaseChatModel: ...
def get_chat_model(config_or_name: LLMConfig | str) -> BaseChatModel:
    config = LLMConfig(name=config_or_name) if isinstance(config_or_name, str) else config_or_name
    timeout = 240 if is_reasoning_model(config.name) else 30
    if is_openai_model(config.name):
        return ChatOpenAI(
            model=config.name,
            timeout=timeout,
            temperature=config.temperature if not is_reasoning_model(config.name) else None,
            max_completion_tokens=config.max_tokens,
            reasoning_effort=config.reasoning_effort if is_reasoning_model(config.name) else None,
            seed=config.seed,
            **config.model_kwargs,
        )
    elif is_anthropic_model(config.name):
        return ChatAnthropic(
            model_name=config.name,
            timeout=timeout,
            temperature=config.temperature,
            max_tokens_to_sample=config.max_tokens,
            **config.model_kwargs,
        )
    elif is_gemini_model(config.name):
        return ChatGoogleGenerativeAI(
            model=config.name,
            timeout=timeout,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            **config.model_kwargs,
        )
    else:
        model: BaseChatModel = init_chat_model(
            config.name,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout=timeout,
            **config.model_kwargs,
        )
        return model


def model_bind_tools(
    model: BaseChatModel, tools: Sequence[BaseTool], **kwargs: Any
) -> Runnable[LanguageModelInput, BaseMessage]:
    if isinstance(model, ChatOpenAI):
        return model.bind_tools(tools, strict=True, **kwargs)
    else:
        return model.bind_tools(tools, **kwargs)


def set_anthropic_cache_breakpoint(content: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(content, str):
        return {"type": "text", "text": content, "cache_control": {"type": "ephemeral"}}
    elif isinstance(content, dict):
        d = content.copy()
        d["cache_control"] = {"type": "ephemeral"}
        return d
    else:
        raise ValueError(f"Unknown content type: {type(content)}")


def set_message_cache_breakpoint(config: LLMConfig, message: BaseMessage) -> BaseMessage:
    """Enable prompt caching for this message (for Anthropic models).

    If you have a list of messages, set a breakpoint only on the last message to automatically
    cache all previous messages.

    See https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
    > Prompt caching references the entire prompt - tools, system, and messages (in that order) up to and including
        the block designated with cache_control.
    """
    if not is_anthropic_model(config.name):
        return message
    new_content: list[dict[str, Any] | str]
    match message.content:
        case str() | dict():
            new_content = [set_anthropic_cache_breakpoint(message.content)]
        case list():
            # Set checkpoint only for the last message
            new_content = message.content.copy()
            new_content[-1] = set_anthropic_cache_breakpoint(new_content[-1])
    return message.model_copy(update={"content": new_content})


def apply_system_prompt_caching(config: LLMConfig, messages: list[BaseMessage]) -> list[BaseMessage]:
    """Apply system prompt caching for Anthropic models."""
    if not (config.cache_system_prompt and is_anthropic_model(config.name)):
        return messages
    # Assume only the first message can be a system prompt.
    assert all(m.type != "system" for m in messages[1:])
    if messages[0].type == "system":
        messages = [set_message_cache_breakpoint(config, messages[0])] + messages[1:]
    return messages


def _call_model(model: Runnable[list[BaseMessage], Any], messages: list[BaseMessage]) -> Any:
    return model.with_retry(wait_exponential_jitter=True, stop_after_attempt=3).invoke(messages)


def chat(
    messages: list[BaseMessage],
    config: LLMConfig,
    model: Runnable[list[BaseMessage], Any] | None = None,
) -> list[BaseMessage]:
    if model is None:
        model = get_chat_model(config)
    messages = apply_system_prompt_caching(config, messages)
    response: AIMessage = _call_model(model, messages)
    return messages + [response]
