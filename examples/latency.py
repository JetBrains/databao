import time
from pathlib import Path

import duckdb
import pandas as pd

import databao

file_path = Path(__file__).parent


def run_scenario() -> bool:
    db_path = file_path / "web_shop_orders/data/web_shop.duckdb"
    conn = duckdb.connect(db_path, read_only=True)

    llm_config = databao.LLMConfig.from_yaml(file_path / "configs/gpt-oss-20b-ollama.yaml")
    # llm_config = databao.LLMConfig(name="claude-sonnet-4-5")

    # llm_config = databao.LLMConfig.from_yaml("configs/qwen3-8b-ollama.yaml")  # Use a custom config file
    # llm_config = databao.LLMConfigDirectory.QWEN3_8B_OLLAMA  # Use one of the preconfigured configs
    agent = databao.new_agent("my_agent", llm_config=llm_config, stream_ask=False)
    agent.add_db(conn)

    thread = agent.thread(lazy=True)
    thread.ask(
        """
        Compute a KPI overview
        Return:
          - total orders
          - total revenue from all orders
          - average order value (AOV)
          - total freight
          - average delivery days (only for delivered orders)
          - average review score (satisfaction proxy)
        """
    )
    thread.ask("count cancelled shows by directors")
    df = thread.df()

    return df is not None


def main() -> None:
    time_measures = []
    for _ in range(10):
        start_time = time.time()
        success = run_scenario()
        if success:
            time_measures.append(time.time() - start_time)

    measures = pd.Series(time_measures)

    print(f"Mean time: {measures.mean():.2f} seconds, Std: {measures.std():.2f}")


if __name__ == "__main__":
    main()
