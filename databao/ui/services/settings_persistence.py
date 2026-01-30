"""Settings persistence service for saving and loading app settings."""

import logging
from pathlib import Path

from databao.ui.models.settings import Settings
from databao.ui.services.storage import (
    find_existing_storage,
    get_settings_path,
    get_storage_base_path,
)

logger = logging.getLogger(__name__)


def save_settings(settings: Settings, base_path: Path | None = None) -> None:
    """Save settings to YAML file.

    Args:
        settings: The Settings object to save
        base_path: Optional base path override
    """
    path = get_settings_path(base_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    yaml_content = settings.to_yaml()
    path.write_text(yaml_content)
    logger.info(f"Settings saved to {path}")


def load_settings(base_path: Path | None = None) -> Settings | None:
    """Load settings from YAML file.

    Args:
        base_path: Optional base path override. If None, searches for existing storage.

    Returns:
        Settings object if found, None otherwise
    """
    # If no base path provided, try to find existing storage
    if base_path is None:
        existing = find_existing_storage()
        if existing is None:
            return None
        base_path = existing

    path = get_settings_path(base_path)
    if not path.exists():
        return None

    try:
        yaml_content = path.read_text()
        settings = Settings.from_yaml(yaml_content)
        # Store the actual base path in settings
        settings.storage.base_path = str(base_path)
        logger.info(f"Settings loaded from {path}")
        return settings
    except Exception as e:
        logger.error(f"Failed to load settings from {path}: {e}")
        return None


def delete_settings(base_path: Path | None = None) -> bool:
    """Delete settings file (for reset to defaults).

    Args:
        base_path: Optional base path override

    Returns:
        True if settings were deleted, False otherwise
    """
    base = base_path or get_storage_base_path()
    path = get_settings_path(base)

    if path.exists():
        try:
            path.unlink()
            logger.info(f"Settings deleted: {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete settings: {e}")
            return False
    return False


def get_or_create_settings() -> Settings:
    """Get existing settings or create new defaults.

    Returns:
        Settings object (either loaded or new defaults)
    """
    settings = load_settings()
    if settings is None:
        settings = Settings()
        # Set the storage path
        settings.storage.base_path = str(get_storage_base_path())
    return settings
