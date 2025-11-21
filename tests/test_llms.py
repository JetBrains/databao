import warnings
from pathlib import Path

import pytest
from langchain_core.language_models import BaseChatModel

from databao.configs import LLMConfigDirectory
from databao.configs.llm import LLMConfig, _parse_model_provider

example_llm_config_paths = [
    p for p in Path(__file__).parent.parent.glob("examples/configs/*.yaml") if not p.name.startswith(".")
]


def _validate_llm_config(config: LLMConfig) -> None:
    if _parse_model_provider(config.name)[0] == "ollama":
        # Avoid downloading models during tests
        config = config.model_copy(update={"ollama_pull_model": False})
        config.model_kwargs["validate_model_on_init"] = False

    try:
        assert isinstance(config.new_chat_model(), BaseChatModel)
    except ConnectionError as e:
        if "ollama" in str(e):
            # ollama needs to be running locally
            warnings.warn("Skipping ollama test due to connection error", stacklevel=2)
        else:
            raise e


@pytest.mark.parametrize("path", example_llm_config_paths, ids=[path.name for path in example_llm_config_paths])
def test_example_llm_configs(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    config = LLMConfig.from_yaml(path)
    _validate_llm_config(config)


@pytest.mark.parametrize(
    "config",
    LLMConfigDirectory.list_all(),
    ids=[c.name for c in LLMConfigDirectory.list_all()],
)
def test_llm_config_directory(config: LLMConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    _validate_llm_config(config)


@pytest.mark.parametrize(
    "model_name,expected_provider,expected_name",
    [
        ("gpt-3.5-turbo", "openai", "gpt-3.5-turbo"),
        ("gpt-4o-mini", "openai", "gpt-4o-mini"),
        ("claude-sonnet-4-20250514", "anthropic", "claude-sonnet-4-20250514"),
        ("ollama:gpt-oss:20b", "ollama", "gpt-oss:20b"),
        ("qwen/qwen3-8b", "", "qwen/qwen3-8b"),
        ("openai/gpt-oss-20b", "", "openai/gpt-oss-20b"),
    ],
)
def test_various_model_names(model_name: str, expected_provider: str, expected_name: str) -> None:
    provider, name = _parse_model_provider(model_name)
    assert provider == expected_provider
    assert name == expected_name
