"""Example: benchmarking a databao agent.

Setup:
1. Copy .env.template to .env and configure your database connection.
2. Run: cd examples/benchmark_template && uv run examples/databao_benchmark.py
"""

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd
from benchmark.core import make_benchmark_cli, run_benchmark, run_benchmark_cli
from benchmark.db import create_databao_domain, create_runner

import databao.agent as bao
from databao.agent import LLMConfig

DEFAULT_SQL_MODEL = "gpt-5.4"


def run(
    input_csv: Path,
    output_csv: Path,
    limit: int | None,
    sql_model: str,
    judge_model: str,
    max_concurrent: int = 8,
    rows: list[int] | None = None,
) -> pd.DataFrame:
    db_runner = create_runner()
    llm_config = LLMConfig(name=sql_model, temperature=0)

    def run_databao_query_sync(question: str) -> tuple[bool, str | None, Any]:
        domain = create_databao_domain(db_runner)
        agent = bao.agent(domain=domain, llm_config=llm_config, stream_ask=False)
        thread = agent.thread()
        thread.ask(question)
        return True, thread.code(), thread.df()

    async def predict_fn(question: str) -> tuple[bool, str | None, Any]:
        return await asyncio.to_thread(run_databao_query_sync, question)

    return run_benchmark(
        input_csv=input_csv,
        output_csv=output_csv,
        limit=limit,
        sql_model=sql_model,
        judge_model=judge_model,
        db_runner=db_runner,
        predict_fn=predict_fn,
        max_concurrent=max_concurrent,
        rows=rows,
    )


def main() -> None:
    parser = make_benchmark_cli(
        description="Run databao agent benchmark.",
        default_sql_model=DEFAULT_SQL_MODEL,
    )
    args = parser.parse_args()
    run_benchmark_cli(args, run)


if __name__ == "__main__":
    main()
