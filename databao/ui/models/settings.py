"""Settings models for app configuration persistence."""

from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class AgentSettings:
    """Agent-related settings."""

    executor_type: str = "lighthouse"


@dataclass
class Settings:
    """Unified settings object combining all setting categories."""

    agent: AgentSettings = field(default_factory=AgentSettings)

    def to_dict(self) -> dict[str, Any]:
        """Convert settings to a dictionary for serialization."""
        return {
            "agent": {
                "executor_type": self.agent.executor_type,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Settings":
        """Create Settings from a dictionary."""
        agent_data = data.get("agent", {})

        return cls(
            agent=AgentSettings(
                executor_type=agent_data.get("executor_type", "lighthouse"),
            ),
        )

    def to_yaml(self) -> str:
        """Serialize settings to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "Settings":
        """Deserialize settings from YAML string."""
        data = yaml.safe_load(yaml_str) or {}
        return cls.from_dict(data)
