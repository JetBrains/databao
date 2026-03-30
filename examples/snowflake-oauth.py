import os
from typing import NoReturn

from databao_context_engine import SnowflakeConnectionProperties, SnowflakeOAuthAuth

import databao.agent as bao


def fail(message: str) -> NoReturn:
    raise RuntimeError(message)


def from_env(key: str) -> str:
    return os.getenv(key) or fail(f"{key} is not set")


def main() -> None:
    domain = bao.domain()
    domain.add_db(
        SnowflakeConnectionProperties(
            user=from_env("SNOWFLAKE_USER"),
            account=from_env("SNOWFLAKE_ACCOUNT"),
            database="CALIFORNIA_TRAFFIC_COLLISION",
            auth=SnowflakeOAuthAuth(token=from_env("SNOWFLAKE_OAUTH_TOKEN")),
        )
    )

    agent = bao.agent(domain=domain, name="my_agent", llm_config=bao.LLMConfig(name="gpt-5.1", temperature=0))

    agent.thread().ask("How many accidents occurred in total?")


if __name__ == "__main__":
    main()
