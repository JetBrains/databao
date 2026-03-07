"""Unit tests for HistoryMode and Visualizer._enrich_with_history_context."""

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage

from databao.agent.core.executor import ExecutionResult
from databao.agent.core.visualizer import HistoryMode, VisualisationResult, Visualizer


class _StubVisualizer(Visualizer):
    """Minimal concrete Visualizer so we can test the base-class history logic."""

    def _visualize(self, request: str, data: ExecutionResult, *, stream: bool = False) -> VisualisationResult:
        raise NotImplementedError

    def edit(self, request: str, visualization: VisualisationResult, *, stream: bool = False) -> VisualisationResult:
        raise NotImplementedError


def _make_execution_result(
    messages: list[BaseMessage],
    text: str = "Final answer text",
) -> ExecutionResult:
    return ExecutionResult(
        text=text,
        meta={ExecutionResult.META_MESSAGES_KEY: messages},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def messages_3_turns() -> list[BaseMessage]:
    """Simulate 3-turn executor history (System + 3x Human/AI/Tool groups)."""
    return [
        SystemMessage("You are a helpful agent."),
        # Turn 1
        HumanMessage("What is the total revenue?"),
        AIMessage("Let me query that."),
        ToolMessage("revenue=930", tool_call_id="tc1"),
        AIMessage("The total revenue is 930."),
        ToolMessage("Submitted.", tool_call_id="tc2"),
        # Turn 2
        HumanMessage("Break it down by category"),
        AIMessage("Querying by category."),
        ToolMessage("A=430, B=500", tool_call_id="tc3"),
        AIMessage("Category A: 430, Category B: 500."),
        ToolMessage("Submitted.", tool_call_id="tc4"),
        # Turn 3
        HumanMessage("Show me the top customers"),
        AIMessage("Querying top customers."),
        ToolMessage("customer_id,ltv\n1,100\n2,90", tool_call_id="tc5"),
        AIMessage("Here are the top customers."),
        ToolMessage("Submitted.", tool_call_id="tc6"),
    ]


@pytest.fixture()
def data_3_turns(messages_3_turns: list[BaseMessage]) -> ExecutionResult:
    return _make_execution_result(messages_3_turns, text="Here are the top customers.")


@pytest.fixture()
def messages_1_turn() -> list[BaseMessage]:
    """Single-turn history: just System + one Human/AI exchange."""
    return [
        SystemMessage("You are a helpful agent."),
        HumanMessage("Count the rows"),
        AIMessage("There are 42 rows."),
    ]


@pytest.fixture()
def data_1_turn(messages_1_turn: list[BaseMessage]) -> ExecutionResult:
    return _make_execution_result(messages_1_turn, text="There are 42 rows.")


INSTRUCTIONS = "Draw a bar chart"


# ---------------------------------------------------------------------------
# NONE
# ---------------------------------------------------------------------------


class TestNone:
    def test_returns_original_request(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.NONE)
        assert viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns) == INSTRUCTIONS

    def test_returns_original_when_no_messages(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.NONE)
        data = ExecutionResult(text="x", meta={})
        assert viz._enrich_with_history_context(INSTRUCTIONS, data) == INSTRUCTIONS


# ---------------------------------------------------------------------------
# LAST_QUESTION
# ---------------------------------------------------------------------------


class TestLastQuestion:
    def test_includes_last_question(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "Show me the top customers" in result
        assert "Instructions:" in result
        assert INSTRUCTIONS in result

    def test_does_not_include_earlier_questions(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "What is the total revenue?" not in result
        assert "Break it down by category" not in result

    def test_single_turn(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_1_turn)

        assert "Count the rows" in result
        assert INSTRUCTIONS in result

    def test_returns_original_when_no_human_messages(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        data = _make_execution_result([SystemMessage("sys"), AIMessage("ai")])
        assert viz._enrich_with_history_context(INSTRUCTIONS, data) == INSTRUCTIONS

    def test_always_has_instructions_section(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "User question history:" in result
        assert "Instructions:" in result
        assert INSTRUCTIONS in result


# ---------------------------------------------------------------------------
# LAST_QUESTION_ANSWER
# ---------------------------------------------------------------------------


class TestLastQuestionAnswer:
    def test_includes_question_and_answer(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "Show me the top customers" in result
        assert "Here are the top customers." in result
        assert INSTRUCTIONS in result

    def test_no_answer_when_text_empty(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        data = _make_execution_result(
            [SystemMessage("sys"), HumanMessage("question")],
            text="",
        )
        result = viz._enrich_with_history_context(INSTRUCTIONS, data)

        assert "question" in result
        # No answer line since text is empty
        assert result.count("\n") < 5  # compact output

    def test_single_turn(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_1_turn)

        assert "Count the rows" in result
        assert "There are 42 rows." in result


# ---------------------------------------------------------------------------
# ALL_QUESTIONS
# ---------------------------------------------------------------------------


class TestAllQuestions:
    def test_includes_all_questions(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "What is the total revenue?" in result
        assert "Break it down by category" in result
        assert "Show me the top customers" in result
        assert INSTRUCTIONS in result

    def test_no_system_or_tool_content(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_3_turns)

        assert "You are a helpful agent" not in result
        assert "Submitted" not in result

    def test_single_turn(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        result = viz._enrich_with_history_context(INSTRUCTIONS, data_1_turn)

        assert "Count the rows" in result


# ---------------------------------------------------------------------------
# Default mode
# ---------------------------------------------------------------------------


def test_default_mode_is_last_question() -> None:
    viz = _StubVisualizer()
    assert viz.history_mode == HistoryMode.LAST_QUESTION


# ---------------------------------------------------------------------------
# Edge cases shared across non-NONE modes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    [
        HistoryMode.LAST_QUESTION,
        HistoryMode.LAST_QUESTION_ANSWER,
        HistoryMode.ALL_QUESTIONS,
    ],
)
def test_returns_original_when_only_system_messages(mode: HistoryMode) -> None:
    viz = _StubVisualizer(history_mode=mode)
    data = _make_execution_result([SystemMessage("sys")])
    assert viz._enrich_with_history_context(INSTRUCTIONS, data) == INSTRUCTIONS


@pytest.mark.parametrize(
    "mode",
    [
        HistoryMode.LAST_QUESTION,
        HistoryMode.LAST_QUESTION_ANSWER,
        HistoryMode.ALL_QUESTIONS,
    ],
)
def test_returns_original_when_no_messages_key(mode: HistoryMode) -> None:
    viz = _StubVisualizer(history_mode=mode)
    data = ExecutionResult(text="x", meta={})
    assert viz._enrich_with_history_context(INSTRUCTIONS, data) == INSTRUCTIONS
