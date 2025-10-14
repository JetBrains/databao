from functools import cached_property
from typing import Any

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, Field


class LLMConfig(BaseModel):
    """Base class with all fields and computed logic for LLM configurations."""

    model_config = ConfigDict(frozen=True)

    # Fields declared in parent - can be overridden in children with different defaults
    name: str
    temperature: float
    max_tokens: int
    reasoning_effort: str
    seed: int
    cache_system_prompt: bool
    model_kwargs: dict[str, Any] = Field(
        default_factory=dict, description="Additional kwargs for the model constructor."
    )

    # Private class constants for model type detection
    _REASONING_MODEL_PREFIXES = ("o1", "o3", "gpt-5")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @property
    def is_reasoning_model(self) -> bool:
        """Check if this is a reasoning model based on the model name."""
        return any(prefix in self.name for prefix in self._REASONING_MODEL_PREFIXES)

    @cached_property
    def chat_model(self) -> BaseChatModel:
        """Create a chat model from this config using init_chat_model for provider detection."""
        timeout = 240 if self.is_reasoning_model else 30

        # Build kwargs dict for the model
        kwargs: dict[str, Any] = {
            "temperature": self.temperature if not self.is_reasoning_model else None,
            "timeout": timeout,
            "max_tokens": self.max_tokens,
            "seed": self.seed,
            **self.model_kwargs,
        }

        # Add reasoning-specific parameters for OpenAI o1/o3 models
        if self.is_reasoning_model:
            kwargs["reasoning_effort"] = self.reasoning_effort

        # Use init_chat_model to automatically detect and instantiate the correct provider
        # This supports OpenAI, Anthropic, Google, Azure, Cohere, Fireworks, Together, Groq, and more
        return init_chat_model(
            model=self.name,
            configurable_fields=None,  # Ensures we match the BaseChatModel overload
            **kwargs,
        )


# TODO: add a config folder for LLM configs, make it initializable from hydra configs
class DefaultLLMConfig(LLMConfig):
    """Lightweight LLM configuration with essential fields and default values."""

    model_config = ConfigDict(frozen=True)

    name: str = "gpt-4o-mini"  # TODO: maybe a better default?
    temperature: float = 0.0
    max_tokens: int = 8192
    reasoning_effort: str = Field(
        default="medium",
        description="Reasoning effort is used for OpenAI reasoning models only. "
        "Warning: reasoning can use a lot of tokens! OpenAI recommends at least 25000 tokens",
    )
    seed: int = 7
    cache_system_prompt: bool = Field(
        default=True, description="Cache system prompt with prompt caching. Only used for Anthropic models."
    )
