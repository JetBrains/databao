"""Unit tests for HistoryMode and Visualizer._extract_history."""

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage

from databao.core.executor import ExecutionResult
from databao.core.visualizer import HistoryMode, VisualisationResult, Visualizer


class _StubVisualizer(Visualizer):
    """Minimal concrete Visualizer so we can test the base-class history logic."""

    def _visualize(
        self, request: str | None, data: ExecutionResult, *, history: list[BaseMessage], stream: bool = False
    ) -> VisualisationResult:
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


# ---------------------------------------------------------------------------
# NONE
# ---------------------------------------------------------------------------


class TestNone:
    def test_returns_empty(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.NONE)
        assert viz._extract_history(data_3_turns) == []

    def test_returns_empty_when_no_messages(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.NONE)
        data = ExecutionResult(text="x", meta={})
        assert viz._extract_history(data) == []


# ---------------------------------------------------------------------------
# LAST_QUESTION
# ---------------------------------------------------------------------------


class TestLastQuestion:
    def test_single_human_message(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        history = viz._extract_history(data_3_turns)

        assert len(history) == 1
        assert isinstance(history[0], HumanMessage)
        assert "Show me the top customers" in str(history[0].content)
        assert "Additional context" in str(history[0].content)

    def test_single_turn(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        history = viz._extract_history(data_1_turn)

        assert len(history) == 1
        assert "Count the rows" in str(history[0].content)

    def test_empty_when_no_messages(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        data = ExecutionResult(text="x", meta={})
        assert viz._extract_history(data) == []

    def test_empty_when_no_human_messages(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION)
        data = _make_execution_result([SystemMessage("sys"), AIMessage("ai")])
        assert viz._extract_history(data) == []


# ---------------------------------------------------------------------------
# LAST_QUESTION_ANSWER
# ---------------------------------------------------------------------------


class TestLastQuestionAnswer:
    def test_human_and_ai_messages(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        history = viz._extract_history(data_3_turns)

        assert len(history) == 2
        assert isinstance(history[0], HumanMessage)
        assert isinstance(history[1], AIMessage)
        assert "Show me the top customers" in str(history[0].content)
        assert "Original agent response" in str(history[1].content)
        assert "Here are the top customers." in str(history[1].content)

    def test_only_human_when_text_empty(self) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        data = _make_execution_result(
            [SystemMessage("sys"), HumanMessage("question")],
            text="",
        )
        history = viz._extract_history(data)

        assert len(history) == 1
        assert isinstance(history[0], HumanMessage)

    def test_single_turn(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.LAST_QUESTION_ANSWER)
        history = viz._extract_history(data_1_turn)

        assert len(history) == 2
        assert "Count the rows" in str(history[0].content)
        assert "There are 42 rows." in str(history[1].content)


# ---------------------------------------------------------------------------
# ALL_QUESTIONS
# ---------------------------------------------------------------------------


class TestAllQuestions:
    def test_all_questions_merged(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        history = viz._extract_history(data_3_turns)

        assert len(history) == 1
        assert isinstance(history[0], HumanMessage)
        content = str(history[0].content)
        assert "1. What is the total revenue?" in content
        assert "2. Break it down by category" in content
        assert "3. Show me the top customers" in content

    def test_single_turn_still_numbered(self, data_1_turn: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        history = viz._extract_history(data_1_turn)

        assert len(history) == 1
        content = str(history[0].content)
        assert "1. Count the rows" in content

    def test_no_system_or_tool_messages_in_output(self, data_3_turns: ExecutionResult) -> None:
        viz = _StubVisualizer(history_mode=HistoryMode.ALL_QUESTIONS)
        history = viz._extract_history(data_3_turns)

        content = str(history[0].content)
        assert "You are a helpful agent" not in content
        assert "Submitted" not in content


# ---------------------------------------------------------------------------
# No human messages edge case (shared across modes that need them)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    [
        HistoryMode.LAST_QUESTION,
        HistoryMode.LAST_QUESTION_ANSWER,
        HistoryMode.ALL_QUESTIONS,
    ],
)
def test_returns_empty_when_only_system_messages(mode: HistoryMode) -> None:
    viz = _StubVisualizer(history_mode=mode)
    data = _make_execution_result([SystemMessage("sys")])
    assert viz._extract_history(data) == []
