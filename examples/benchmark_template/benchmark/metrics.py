from __future__ import annotations

import json

from openai import AsyncOpenAI
from ragas.metrics.discrete import discrete_metric
from ragas.metrics.result import MetricResult

from benchmark.helpers import df_to_markdown

try:
    from langsmith.wrappers import wrap_openai
except ImportError:
    wrap_openai = None


LLM_JUDGE_PROMPT = """Compare `Generated Dataframe` to `Gold Dataframe` to check if the Question was answered correctly.

Rules:
- The Gold Dataframe is ground truth.
- Ignore column names, column order, and meaningful rounding differences.
- Additional information is OK unless it contradicts the Gold Dataframe.
- Assign a verdict:
  - "correct": all important data from Gold is present in Generated.
  - "partially": minor value errors or different aggregation granularity.
  - "wrong": important data is missing or numbers differ significantly.

Question:
{question}

Gold Dataframe:
{gold_df}

Generated Dataframe:
{generated_df}

Respond with JSON: {{"verdict": "correct"|"partially"|"wrong", "reason": "..."}}
"""


def make_metrics(judge_model: str):
    """Create and return (llm_judge, execution_accuracy) metric functions."""
    from datacompy.core import Compare

    client = AsyncOpenAI()
    if wrap_openai is not None:
        client = wrap_openai(client, chat_name="LLM Judge")

    @discrete_metric(name="llm_judge", allowed_values=["correct", "partially", "wrong"])
    async def llm_judge(question: str, gold_df, generated_df):
        if gold_df is None:
            return MetricResult(value="wrong", reason="Gold DF is None")
        if generated_df is None:
            return MetricResult(value="wrong", reason="Generated DF is None")
        try:
            resp = await client.chat.completions.create(
                model=judge_model,
                messages=[
                    {
                        "role": "user",
                        "content": LLM_JUDGE_PROMPT.format(
                            question=question,
                            gold_df=df_to_markdown(gold_df),
                            generated_df=df_to_markdown(generated_df),
                        ),
                    }
                ],
                response_format={"type": "json_object"},
            )
            result = json.loads(resp.choices[0].message.content)
            verdict = result.get("verdict", "wrong").lower()
            if verdict not in ("correct", "partially", "wrong"):
                verdict = "wrong"
            return MetricResult(value=verdict, reason=result.get("reason", ""))
        except Exception as e:
            return MetricResult(value="wrong", reason=f"Judge error: {e}")

    @discrete_metric(name="execution_accuracy", allowed_values=["correct", "incorrect"])
    def execution_accuracy(gold_success, gold_result, pred_success, pred_result):
        if not gold_success:
            return MetricResult(value="incorrect", reason=f"Gold SQL failed: {gold_result}")
        if not pred_success:
            return MetricResult(value="incorrect", reason=f"Predicted SQL failed: {pred_result}")
        if gold_result.empty and pred_result.empty:
            return MetricResult(value="correct", reason="Both empty")
        if gold_result.empty != pred_result.empty:
            return MetricResult(value="incorrect", reason=f"Row count: {len(gold_result)} vs {len(pred_result)}")
        try:
            cmp = Compare(
                gold_result.reset_index(drop=True),
                pred_result.reset_index(drop=True),
                on_index=True,
                abs_tol=0.01,
                rel_tol=0.001,
                df1_name="gold",
                df2_name="predicted",
            )
            if cmp.matches():
                return MetricResult(value="correct", reason=f"Match: {len(gold_result)} rows")
            return MetricResult(value="incorrect", reason="DataFrames differ")
        except Exception as e:
            return MetricResult(value="incorrect", reason=f"Compare error: {e}")

    return llm_judge, execution_accuracy
