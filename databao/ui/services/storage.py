"""Storage service for determining and managing storage paths."""

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Storage directory name
_STORAGE_DIR_NAME = ".databao"
_UI_SUBDIR = "ui"

# Subdirectories within the storage
_CHATS_SUBDIR = "chats"
_CACHE_SUBDIR = "cache"
_SETTINGS_FILE = "settings.yaml"


def _is_writable(path: Path) -> bool:
    """Check if a path is writable."""
    try:
        # Try to create the directory if it doesn't exist
        path.mkdir(parents=True, exist_ok=True)
        # Try to create a test file
        test_file = path / ".write_test"
        test_file.touch()
        test_file.unlink()
        return True
    except (OSError, PermissionError):
        return False


def _get_home_storage_path() -> Path:
    """Get the storage path in user's home directory."""
    return Path.home() / _STORAGE_DIR_NAME / _UI_SUBDIR


def _get_tmp_storage_path() -> Path:
    """Get the storage path in temp directory."""
    return Path(tempfile.gettempdir()) / _STORAGE_DIR_NAME / _UI_SUBDIR


def get_storage_base_path() -> Path:
    """Get the base storage path, preferring home directory.

    Returns:
        Path to ~/.databao/ui/ if writable, otherwise /tmp/.databao/ui/
    """
    home_path = _get_home_storage_path()
    if _is_writable(home_path):
        return home_path

    tmp_path = _get_tmp_storage_path()
    if _is_writable(tmp_path):
        logger.warning(f"Home directory not writable, using temp: {tmp_path}")
        return tmp_path

    # Last resort - should rarely happen
    raise RuntimeError("Cannot find writable storage location")


def find_existing_storage() -> Path | None:
    """Find existing storage directory, prioritizing home over tmp.

    This is used during startup to detect where settings/chats are stored.

    Returns:
        Path to existing storage, or None if no storage found.
    """
    home_path = _get_home_storage_path()
    if home_path.exists() and (home_path / _SETTINGS_FILE).exists():
        return home_path

    tmp_path = _get_tmp_storage_path()
    if tmp_path.exists() and (tmp_path / _SETTINGS_FILE).exists():
        return tmp_path

    # Also check for chats directory as indicator of existing storage
    if home_path.exists() and (home_path / _CHATS_SUBDIR).exists():
        return home_path

    if tmp_path.exists() and (tmp_path / _CHATS_SUBDIR).exists():
        return tmp_path

    return None


def get_settings_path(base_path: Path | None = None) -> Path:
    """Get the path to settings.yaml file.

    Args:
        base_path: Optional base path override. If None, auto-detected.

    Returns:
        Path to settings.yaml
    """
    base = base_path or get_storage_base_path()
    return base / _SETTINGS_FILE


def get_chats_dir(base_path: Path | None = None) -> Path:
    """Get the chats directory path.

    Args:
        base_path: Optional base path override. If None, auto-detected.

    Returns:
        Path to chats directory
    """
    base = base_path or get_storage_base_path()
    chats_dir = base / _CHATS_SUBDIR
    chats_dir.mkdir(parents=True, exist_ok=True)
    return chats_dir


def get_cache_dir(base_path: Path | None = None) -> Path:
    """Get the cache directory path for DiskCache.

    Args:
        base_path: Optional base path override. If None, auto-detected.

    Returns:
        Path to cache directory
    """
    base = base_path or get_storage_base_path()
    cache_dir = base / _CACHE_SUBDIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_chat_dir(chat_id: str, base_path: Path | None = None) -> Path:
    """Get the directory for a specific chat.

    Args:
        chat_id: The chat's unique ID
        base_path: Optional base path override. If None, auto-detected.

    Returns:
        Path to the chat's directory
    """
    chats = get_chats_dir(base_path)
    chat_dir = chats / chat_id
    chat_dir.mkdir(parents=True, exist_ok=True)
    return chat_dir


def ensure_storage_exists(base_path: Path | None = None) -> Path:
    """Ensure the storage directory structure exists.

    Args:
        base_path: Optional base path override. If None, auto-detected.

    Returns:
        The base storage path
    """
    base = base_path or get_storage_base_path()
    base.mkdir(parents=True, exist_ok=True)
    get_chats_dir(base)
    get_cache_dir(base)
    return base
