import os
from pathlib import Path
from typing import NoReturn

from sqlalchemy import create_engine, text

import databao
from databao import LLMConfig

FILE_DIR = Path(__file__).parent


def fail(message: str) -> NoReturn:
    raise RuntimeError(message)


def from_env(key: str) -> str:
    return os.getenv(key) or fail(f"{key} is not set")


def main() -> None:
    engine = create_engine(
        "snowflake://{user}@{account_identifier}/{database}?private_key_file={private_key_file}".format(
            user=from_env("SNOWFLAKE_USER"),
            # password=from_env("SNOWFLAKE_PASSWORD"),
            account_identifier=from_env("SNOWFLAKE_ACCOUNT"),
            database="CALIFORNIA_TRAFFIC_COLLISION",
            private_key_file=from_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
        )
    )

    with engine.connect() as db_connection:
        result = db_connection.execute(text("select current_version();")).fetchone()

        if result is None:
            fail("Failed to execute query")

        print(f"Snowflake version: {result[0]}")

    project_dir = Path(FILE_DIR, "example-dce-project")

    if not project_dir.is_dir():
        project_dir.mkdir(parents=True)

    domain = databao.domain(project_dir)
    domain.add_source(engine)

    agent = databao.agent(domain=domain, name="my_agent", llm_config=LLMConfig(name="gpt-5.1", temperature=0))

    agent.thread().ask("How many accidents occurred in total?")


if __name__ == "__main__":
    main()
