class DbtError(RuntimeError):
    """Base error for databao dbt integration."""


class DbtNotEnabledError(DbtError):
    """Raised when dbt functionality is called but dbt_config is not set on the Agent."""


class DbtMissingWarehouseError(DbtError):
    """Raised when a dbt executor is started without any database or dataframe warehouse connection."""

    def __init__(self) -> None:
        super().__init__(
            "DbtProjectExecutor requires at least one database or dataframe source in addition to a dbt project. "
            "Register a warehouse connection via domain.add_db() alongside domain.add_dbt()."
        )
