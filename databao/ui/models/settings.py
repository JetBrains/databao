"""Settings models for app configuration persistence."""

from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class AgentSettings:
    """Agent-related settings."""

    executor_type: str = "lighthouse"


@dataclass
class ProjectSettings:
    """DCE project settings."""

    dce_project_path: str | None = None


@dataclass
class StorageSettings:
    """Storage-related settings (mostly read-only in UI)."""

    # This is computed at runtime, but can be overridden
    base_path: str | None = None


@dataclass
class Settings:
    """Unified settings object combining all setting categories."""

    agent: AgentSettings = field(default_factory=AgentSettings)
    project: ProjectSettings = field(default_factory=ProjectSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)

    def to_dict(self) -> dict[str, Any]:
        """Convert settings to a dictionary for serialization."""
        return {
            "agent": {
                "executor_type": self.agent.executor_type,
            },
            "project": {
                "dce_project_path": self.project.dce_project_path,
            },
            "storage": {
                "base_path": self.storage.base_path,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Settings":
        """Create Settings from a dictionary."""
        agent_data = data.get("agent", {})
        project_data = data.get("project", {})
        storage_data = data.get("storage", {})

        return cls(
            agent=AgentSettings(
                executor_type=agent_data.get("executor_type", "lighthouse"),
            ),
            project=ProjectSettings(
                dce_project_path=project_data.get("dce_project_path"),
            ),
            storage=StorageSettings(
                base_path=storage_data.get("base_path"),
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
