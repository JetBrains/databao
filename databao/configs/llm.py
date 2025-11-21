import os
from pathlib import Path
from typing import Any, Literal

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

_OPENAI_PREFIXES = ["gpt", "o1", "o3", "o4"]
_ANTHROPIC_PREFIXES = ["claude", "anthropic"]
_OPENAI_REASONING_INFIXES = ["o1", "o3", "o4", "gpt-5", "openai/gpt-oss"]


# TODO: add a config folder for LLM configs, make it initializable from hydra configs
class LLMConfig(BaseModel):
    """Base class with all fields and computed logic for LLM configurations."""

    # Fields declared in parent - can be overridden in children with different defaults
    name: str
    """The model name can be of the form 'provider:name' or 'name'."""

    temperature: float = 0.0
    max_tokens: int = 8192
    """Maximum number of tokens to generate."""
    reasoning_effort: str = "medium"
    """Reasoning effort is used for OpenAI reasoning models only. 
    Warning: reasoning can use a lot of tokens! OpenAI recommends at least 25000 tokens"""
    cache_system_prompt: bool = True
    """Cache system prompt with prompt caching. Only used for Anthropic models."""
    # TODO multi-turn prompt caching

    max_tokens_before_cleaning: int = 10000
    """Number of tokens to start history cleaning. Each Executor has it's own cleaning strategy."""

    timeout: int | None | Literal["auto"] = "auto"
    """Timeout in seconds for LLM calls. If None, use the LLM provider's defaults. 
    If 'auto', use a default timeout (60s) that increases for reasoning models."""

    api_base_url: str | None = None
    """Base URL for an OpenAI-compatible API like 'http://localhost:8080/v1'. Mostly used for running local models."""
    use_responses_api: bool = True
    """Use the [responses API](https://platform.openai.com/docs/guides/migrate-to-responses) for OpenAI models. 
    If False, use the old Chat Completions API (useful for local models that don't support the new responses API)."""

    ollama_pull_model: bool = True
    """Pull the model from Ollama if it's not already downloaded. Only applicable for the 'ollama' model provider."""

    model_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Additional kwargs for the model constructor."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    def _resolve_timeout(self) -> float | None:
        if self.timeout == "auto":
            return 360 if _is_reasoning_model(self.name) else 60
        else:
            return self.timeout

    def new_chat_model(self) -> BaseChatModel:
        """Create a chat model from this config using init_chat_model for provider detection."""
        provider, name = _parse_model_provider(self.name)
        if provider == "openai" or self.api_base_url is not None:
            from langchain_openai import ChatOpenAI

            # Use the verbatim name if using an OAI server
            model_name = self.name if self.api_base_url is not None else name

            is_reasoning = _is_reasoning_model(model_name)
            extra_kwargs: dict[str, Any] = {}
            if self.use_responses_api:
                extra_kwargs.update(
                    # Without "summary", no reasoning traces will be returned by the API
                    reasoning={"effort": self.reasoning_effort, "summary": "auto"} if is_reasoning else None,
                    temperature=self.temperature,
                    # TODO output_version="responses/v1"
                )
            else:
                extra_kwargs.update(
                    reasoning_effort=self.reasoning_effort if is_reasoning else None,
                    # The old API errors out if you provide a temperature for reasoning models
                    temperature=self.temperature if not is_reasoning else None,
                )

            # Set a default API key for local models if the user didn't provide one
            if (
                self.api_base_url is not None
                and "api_key" not in self.model_kwargs
                and "OPENAI_API_KEY" not in os.environ
            ):
                extra_kwargs["api_key"] = "local-api-key"

            return ChatOpenAI(
                model=model_name,
                timeout=self._resolve_timeout(),
                max_tokens=self.max_tokens,
                base_url=self.api_base_url,
                use_responses_api=self.use_responses_api,
                **extra_kwargs,
                **self.model_kwargs,
            )
        elif provider == "anthropic":
            from langchain_anthropic import ChatAnthropic

            return ChatAnthropic(
                model_name=name,
                timeout=self._resolve_timeout(),
                temperature=self.temperature,
                max_tokens_to_sample=self.max_tokens,
                **self.model_kwargs,
            )

        if provider == "ollama" and self.ollama_pull_model:
            import ollama

            # Download with ollama. If the model already exists it will not be re-downloaded.
            ollama.pull(name)

        return init_chat_model(
            self.name,
            configurable_fields=None,  # Ensures we match the BaseChatModel overload
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            timeout=self._resolve_timeout(),
            **self.model_kwargs,
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> Self:
        """Load an LLM config from a YAML file."""

        import yaml

        path = Path(path)
        if path.exists():
            content = path.read_text()
        else:
            raise ValueError(f"LLM config file {path} not found.")

        model_dict = yaml.safe_load(content)
        return cls.model_validate(model_dict)


def _is_reasoning_model(model_name: str) -> bool:
    """Check if a model is a reasoning model based on its name."""
    return any(prefix in model_name for prefix in _OPENAI_REASONING_INFIXES)


def _is_openai_model(model_name: str) -> bool:
    """Check if a model is an OpenAI model based on its name."""
    return any(model_name.startswith(prefix) for prefix in _OPENAI_PREFIXES)


def _is_anthropic_model(model_name: str) -> bool:
    """Check if a model is an Anthropic model based on its name."""
    return any(model_name.startswith(prefix) for prefix in _ANTHROPIC_PREFIXES)


def _parse_model_provider(model: str) -> tuple[str, str]:
    """Parse the provider and model name from a string of the form 'provider:name' or 'name'."""
    provider, sep, name = model.partition(":")
    if len(sep) == 0 and len(name) == 0:
        if _is_openai_model(model):
            return "openai", model
        elif _is_anthropic_model(model):
            return "anthropic", model
        else:
            return "", model
    return provider, name


class LLMConfigDirectory:
    """Namespace for preconfigured LLM configurations."""

    @classmethod
    def list_all(cls) -> list[LLMConfig]:
        return [config for name, config in vars(cls).items() if name.isupper()]

    DEFAULT = LLMConfig(name="gpt-4o-mini")

    # https://huggingface.co/Qwen/Qwen3-8B-GGUF#best-practices
    QWEN3_8B_OAI = LLMConfig(
        name="qwen/qwen3-8b",
        api_base_url="http://localhost:8080/v1",
        max_tokens=32768,
        temperature=0.6,
        use_responses_api=False,
        timeout=600,
    )

    # https://huggingface.co/Qwen/Qwen3-8B-GGUF#best-practices
    QWEN3_8B_OLLAMA = LLMConfig(
        name="ollama:qwen3:8b",
        max_tokens=32768,
        temperature=0.6,
        timeout=600,
        # Refer to https://python.langchain.com/api_reference/ollama/chat_models/langchain_ollama.chat_models.ChatOllama.html
        model_kwargs={
            "reasoning": True,
            "num_ctx": 40960,  # Override the global context size: https://docs.ollama.com/context-length
            "num_predict": 32768,
            "validate_model_on_init": True,
        },
    )
