from __future__ import annotations

import argparse
import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from ragas import Dataset, experiment

from benchmark.db import DBRunner
from benchmark.helpers import must_env
from benchmark.metrics import make_metrics


def load_benchmark_dataset(
    input_csv: Path, limit: int | None = None, rows: list[int] | None = None
) -> tuple[pd.DataFrame, Any]:
    """Load the gold SQL CSV into a Ragas Dataset.

    Expected CSV columns: user_input, gold_sql, difficulty
    Optional columns: domain
    """
    df_gold = pd.read_csv(input_csv)
    if rows is not None:
        df_gold = df_gold.iloc[rows].reset_index(drop=True)
    elif limit is not None:
        df_gold = df_gold.head(limit).reset_index(drop=True)

    dataset: Any = Dataset(name=os.environ.get("DATASET_NAME", "my_benchmark"), backend="local/csv", root_dir=".")
    for _, row in df_gold.iterrows():
        entry = {
            "user_input": row["user_input"],
            "reference": row["gold_sql"],
            "difficulty": row["difficulty"],
        }
        if "domain" in df_gold.columns:
            entry["domain"] = row["domain"]
        dataset.append(entry)
    return df_gold, dataset


def print_summary(results: pd.DataFrame, model_name: str) -> None:
    """Print a human-readable summary of benchmark results."""
    total = len(results)
    if total == 0:
        print("No rows were benchmarked.")
        return

    exec_correct = int((results["execution_accuracy"] == "correct").sum())
    judge_correct = int((results["llm_judge"] == "correct").sum())
    judge_partial = int((results["llm_judge"] == "partially").sum())

    print(f"{'=' * 60}")
    print(f"RESULTS ({total} queries, model={model_name})")
    print(f"{'=' * 60}")
    print(f"  Execution Accuracy:  {exec_correct}/{total} ({exec_correct / total * 100:.1f}%)")
    print(f"  LLM Judge (correct): {judge_correct}/{total} ({judge_correct / total * 100:.1f}%)")
    print(
        f"  LLM Judge (>=partial): "
        f"{judge_correct + judge_partial}/{total} ({(judge_correct + judge_partial) / total * 100:.1f}%)"
    )

    if "time_s" in results.columns:
        times = results["time_s"].dropna()
        if len(times) > 0:
            print("\n  Timing (seconds):")
            print(f"    Mean:   {times.mean():.1f}")
            print(f"    Median: {times.median():.1f}")
            print(f"    P95:    {times.quantile(0.95):.1f}")
            print(f"    Min:    {times.min():.1f}")
            print(f"    Max:    {times.max():.1f}")

    print()
    print("By Difficulty:")
    for difficulty in results["difficulty"].dropna().unique():
        subset = results[results["difficulty"] == difficulty]
        total_d = len(subset)
        judge_ok_d = int(subset["llm_judge"].isin(["correct", "partially"]).sum())
        print(f"  {difficulty!s:<8} — Judge: {judge_ok_d}/{total_d}")

    if "domain" in results.columns:
        print()
        print("By Domain:")
        for domain in results["domain"].dropna().unique():
            subset = results[results["domain"] == domain]
            total_dom = len(subset)
            judge_ok_dom = int(subset["llm_judge"].isin(["correct", "partially"]).sum())
            print(f"  {domain!s:<8} — Judge: {judge_ok_dom}/{total_dom}")


def run_benchmark(
    input_csv: Path,
    output_csv: Path,
    limit: int | None,
    sql_model: str,
    judge_model: str,
    db_runner: DBRunner,
    predict_fn: Callable[[str], Awaitable[tuple[bool, str | None, Any]]],
    max_concurrent: int = 8,
    rows: list[int] | None = None,
) -> pd.DataFrame:
    """Run the full benchmark: generate SQL, execute gold + predicted, evaluate.

    Args:
        input_csv: Path to CSV with gold SQL queries.
        output_csv: Path to write results CSV.
        limit: Optional row limit for quick tests.
        sql_model: Model name used for SQL generation (for labeling).
        judge_model: OpenAI model for the LLM judge.
        db_runner: Database runner instance (from create_runner()).
        predict_fn: Async function (question) -> (success, sql_or_none, result_df_or_error).
        max_concurrent: Max concurrent benchmark queries.
    """
    must_env("OPENAI_API_KEY")

    _df_gold, dataset = load_benchmark_dataset(input_csv=input_csv, limit=limit, rows=rows)
    llm_judge, execution_accuracy = make_metrics(judge_model=judge_model)

    semaphore = asyncio.Semaphore(max_concurrent)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_csv = output_csv.with_stem(f"{output_csv.stem}__{timestamp}")
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    csv_lock = asyncio.Lock()

    @experiment()
    async def benchmark(row: Any) -> Any:
        async with semaphore:

            async def timed_predict(question: str) -> tuple[tuple[bool, str | None, Any], float]:
                t0 = time.perf_counter()
                try:
                    result = await predict_fn(question)
                except Exception as e:
                    print(f"Error running predict_fn: {e}")
                    result = False, None, str(e)
                return result, time.perf_counter() - t0

            async with asyncio.TaskGroup() as tg:
                pred_task = tg.create_task(timed_predict(row["user_input"]))
                gold_task = tg.create_task(asyncio.to_thread(db_runner.execute_sql, row["reference"]))
            (pred_ok, pred_sql, pred_result), elapsed_s = pred_task.result()
            gold_ok, gold_result = gold_task.result()

        exec_score = await execution_accuracy.ascore(
            gold_success=gold_ok,
            gold_result=gold_result,
            pred_success=pred_ok,
            pred_result=pred_result if pred_ok else str(pred_result),
        )
        judge_score = await llm_judge.ascore(
            question=row["user_input"],
            gold_df=gold_result if gold_ok else None,
            generated_df=pred_result if pred_ok else None,
        )

        result = {
            "query": row["user_input"],
            "difficulty": row["difficulty"],
            "predicted_sql": pred_sql,
            "execution_accuracy": exec_score.value,
            "exec_reason": exec_score.reason,
            "llm_judge": judge_score.value,
            "judge_reason": judge_score.reason,
            "gold_ok": gold_ok,
            "pred_ok": pred_ok,
            "gold_df": str(gold_result),
            "pred_df": str(pred_result),
            "time_s": elapsed_s,
        }

        if "domain" in row:
            result["domain"] = row["domain"]

        row_df = pd.DataFrame([result])
        async with csv_lock:
            row_df.to_csv(timestamped_csv, index=False, mode="a", header=not timestamped_csv.exists())

        return result

    results = asyncio.run(benchmark.arun(dataset, name=f"{output_csv.stem}__{sql_model}__{timestamp}"))
    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)
    return out_df


def make_benchmark_cli(
    description: str,
    default_sql_model: str,
) -> argparse.ArgumentParser:
    """Create an argument parser with common benchmark flags."""
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", default=os.environ.get("INPUT_CSV", "benchmark_questions.csv"), help="Path to benchmark CSV."
    )
    parser.add_argument(
        "--output", default=os.environ.get("OUTPUT_CSV", "results/output.csv"), help="Path to output CSV."
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--limit", type=int, default=None, help="Run only the first N questions.")
    selection.add_argument(
        "--rows",
        type=lambda s: [int(x) for x in s.split(",")],
        default=None,
        help="Comma-separated row indices to run (0-based). E.g. --rows 0,3,7",
    )
    parser.add_argument("--sql-model", default=default_sql_model, help="Model for SQL generation.")
    parser.add_argument(
        "--judge-model", default=os.environ.get("JUDGE_MODEL", "gpt-5.4"), help="OpenAI model for LLM judge."
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=int(os.environ.get("MAX_CONCURRENT", "8")),
        help="Maximum number of concurrent benchmark tasks.",
    )
    return parser


def run_benchmark_cli(args: argparse.Namespace, run_benchmark_fn: Callable[..., pd.DataFrame]) -> None:
    """Run a benchmark from parsed CLI arguments and print summary."""
    from dotenv import load_dotenv

    load_dotenv()

    results = run_benchmark_fn(
        input_csv=Path(args.input),
        output_csv=Path(args.output),
        limit=args.limit,
        rows=args.rows,
        sql_model=args.sql_model,
        judge_model=args.judge_model,
        max_concurrent=args.max_concurrent,
    )
    print_summary(results, model_name=args.sql_model)
    print(f"\nSaved: {args.output}")
