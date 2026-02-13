class DbtError(RuntimeError):
    """Base error for databao dbt integration."""


class DbtNotEnabledError(DbtError):
    """Raised when dbt functionality is called but dbt_config is not set on the Agent."""
