import os
from typing import NoReturn

from sqlalchemy import create_engine

import databao.agent as bao


def fail(message: str) -> NoReturn:
    raise RuntimeError(message)


def from_env(key: str) -> str:
    return os.getenv(key) or fail(f"{key} is not set")


def main() -> None:
    engine = create_engine(
        "snowflake://{user}@{account_identifier}/{database}?private_key_file={private_key_file}&warehouse={warehouse}".format(
            user=from_env("SNOWFLAKE_USER"),
            # password=from_env("SNOWFLAKE_PASSWORD"),
            account_identifier=from_env("SNOWFLAKE_ACCOUNT"),
            database=from_env("SNOWFLAKE_DATABASE"),
            private_key_file=from_env("SNOWFLAKE_PRIVATE_KEY_FILE"),
            warehouse=from_env("SNOWFLAKE_WAREHOUSE"),
        )
    )

    domain = bao.domain()
    domain.add_db(engine)

    agent = bao.agent(domain=domain, name="my_agent", llm_config=bao.LLMConfig(name="gpt-5.1", temperature=0))

    agent.thread().ask("How many accidents occurred in total?")


if __name__ == "__main__":
    main()
