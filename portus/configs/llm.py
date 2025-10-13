from enum import Enum
from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator


class ModelFamily(str, Enum):
    """Enum representing different LLM provider families."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GEMINI = "gemini"


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
    _OPENAI_INFIXES = ("gpt", "o1", "o3")
    _ANTHROPIC_INFIXES = ("claude",)
    _GEMINI_INFIXES = ("gemini",)

    @field_validator("name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        """Validate that the model name is from a supported provider."""
        all_infixes = cls._OPENAI_INFIXES + cls._ANTHROPIC_INFIXES + cls._GEMINI_INFIXES
        if not any(infix in v for infix in all_infixes):
            raise ValueError(f"Unsupported model name: {v}. Model name must contain one of: {', '.join(all_infixes)}")
        return v

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def family(self) -> ModelFamily:
        """Determine the model family based on the model name."""
        if any(infix in self.name for infix in self._OPENAI_INFIXES):
            return ModelFamily.OPENAI
        elif any(infix in self.name for infix in self._ANTHROPIC_INFIXES):
            return ModelFamily.ANTHROPIC
        elif any(infix in self.name for infix in self._GEMINI_INFIXES):
            return ModelFamily.GEMINI
        else:
            # This should never happen due to the validator
            raise ValueError(f"Unsupported model family for: {self.name}")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_reasoning_model(self) -> bool:
        """Check if this is a reasoning model based on the model name."""
        return any(prefix in self.name for prefix in self._REASONING_MODEL_PREFIXES)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def chat_model(self) -> BaseChatModel:
        """Create a chat model from this config."""
        timeout = 240 if self.is_reasoning_model else 30

        match self.family:
            case ModelFamily.OPENAI:
                return ChatOpenAI(
                    model=self.name,
                    timeout=timeout,
                    temperature=self.temperature if not self.is_reasoning_model else None,
                    max_completion_tokens=self.max_tokens,
                    reasoning_effort=self.reasoning_effort if self.is_reasoning_model else None,
                    seed=self.seed,
                    **self.model_kwargs,
                )
            case ModelFamily.ANTHROPIC:
                return ChatAnthropic(
                    model_name=self.name,
                    timeout=timeout,
                    temperature=self.temperature,
                    max_tokens_to_sample=self.max_tokens,
                    **self.model_kwargs,
                )
            case ModelFamily.GEMINI:
                return ChatGoogleGenerativeAI(
                    model=self.name,
                    timeout=timeout,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    **self.model_kwargs,
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
