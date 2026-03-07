"""Tests for system prompt caching and message cache breakpoint helpers."""

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from databao.agent.configs.llm import LLMConfig
from databao.agent.executors.llm import _set_anthropic_cache_breakpoint, apply_system_prompt_caching


@pytest.fixture
def anthropic_config() -> LLMConfig:
    return LLMConfig(name="claude-sonnet-4-20250514", cache_system_prompt=True)


@pytest.fixture
def openai_config() -> LLMConfig:
    return LLMConfig(name="gpt-4o-mini", cache_system_prompt=True)


class TestSetAnthropicCacheBreakpoint:
    """Tests for _set_anthropic_cache_breakpoint helper."""

    def test_string_content(self) -> None:
        result = _set_anthropic_cache_breakpoint("System prompt")
        assert result == {"type": "text", "text": "System prompt", "cache_control": {"type": "ephemeral"}}

    def test_dict_content(self) -> None:
        result = _set_anthropic_cache_breakpoint({"type": "text", "text": "Prompt"})
        assert result == {"type": "text", "text": "Prompt", "cache_control": {"type": "ephemeral"}}


class TestApplySystemPromptCaching:
    """Tests for apply_system_prompt_caching main function."""

    def test_non_anthropic_model_returns_unchanged(self, openai_config: LLMConfig) -> None:
        messages = [SystemMessage(content="System prompt"), HumanMessage(content="User message")]
        result = apply_system_prompt_caching(openai_config, messages)
        assert result is messages

    def test_applies_caching_to_system_message(self, anthropic_config: LLMConfig) -> None:
        messages = [SystemMessage(content="System prompt"), HumanMessage(content="User message")]
        result = apply_system_prompt_caching(anthropic_config, messages)
        assert result[0].content == [{"type": "text", "text": "System prompt", "cache_control": {"type": "ephemeral"}}]

    def test_multi_part_system_message_only_last_cached(self, anthropic_config: LLMConfig) -> None:
        messages = [
            SystemMessage(content=[{"type": "text", "text": "Part 1"}, {"type": "text", "text": "Part 2"}]),
            HumanMessage(content="Question"),
        ]
        result = apply_system_prompt_caching(anthropic_config, messages)
        content = result[0].content
        assert isinstance(content, list)
        first_part, second_part = content[0], content[1]
        assert isinstance(first_part, dict) and isinstance(second_part, dict)
        assert "cache_control" not in first_part
        assert second_part["cache_control"] == {"type": "ephemeral"}
