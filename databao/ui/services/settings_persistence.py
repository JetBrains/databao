"""Settings persistence service for saving and loading app settings."""

import logging

from databao.ui.models.settings import Settings
from databao.ui.services.storage import get_settings_path

logger = logging.getLogger(__name__)


def save_settings(settings: Settings) -> None:
    """Save settings to YAML file.

    Args:
        settings: The Settings object to save
    """
    path = get_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    yaml_content = settings.to_yaml()
    path.write_text(yaml_content)
    logger.info(f"Settings saved to {path}")


def load_settings() -> Settings | None:
    """Load settings from YAML file.

    Returns:
        Settings object if found, None otherwise
    """

    path = get_settings_path()
    if not path.exists():
        return None

    try:
        yaml_content = path.read_text()
        settings = Settings.from_yaml(yaml_content)
        logger.info(f"Settings loaded from {path}")
        return settings
    except Exception as e:
        logger.error(f"Failed to load settings from {path}: {e}")
        return None


def delete_settings() -> bool:
    """Delete settings file (for reset to defaults).

    Returns:
        True if settings were deleted, False otherwise
    """
    path = get_settings_path()

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
    return settings
