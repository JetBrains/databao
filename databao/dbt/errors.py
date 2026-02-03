class DbtError(RuntimeError):
    """Base error for databao dbt integration."""


class DbtNotEnabledError(DbtError):
    """Raised when dbt functionality is called but dbt_config is not set on the Agent."""

class DbtError(RuntimeError):
    """Base error for databao dbt integration."""

class DbtNotEnabledError(DbtError):
    """Raised when dbt functionality is called but dbt_config is not set on the Agent."""

class DbtPlanNotReadyError(DbtError):
    """Raised when a plan action requires a sandbox/run results that are not available yet."""

class DbtApplyNotAllowedError(DbtError):
    """Raised when apply is attempted but the configuration disallows it."""