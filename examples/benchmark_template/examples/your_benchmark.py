"""Template: plug in your own agent.

Setup:
1. Copy .env.template to .env and configure your database connection.
2. Implement predict_fn below.
3. Run: cd examples/benchmark_template && uv run examples/your_benchmark.py
"""

from pathlib import Path
from typing import Any

import pandas as pd
from benchmark.core import make_benchmark_cli, run_benchmark, run_benchmark_cli
from benchmark.db import create_runner

DEFAULT_SQL_MODEL = "gpt-5.4"


async def predict_fn(question: str) -> tuple[bool, str | None, Any]:
    """TODO: Implement your agent's predict function.

    Given a natural-language question, return:
        (success: bool, sql: str | None, result: DataFrame | str)

    - success: True if the agent produced a valid result
    - sql: The generated SQL query (or None if unavailable)
    - result: The query result as a DataFrame, or an error string
    """
    raise NotImplementedError("Implement your predict function")


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
        description="Run your custom benchmark.",
        default_sql_model=DEFAULT_SQL_MODEL,
    )
    args = parser.parse_args()
    run_benchmark_cli(args, run)


if __name__ == "__main__":
    main()
