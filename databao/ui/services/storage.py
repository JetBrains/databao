"""Storage service for determining and managing storage paths."""

import logging
from pathlib import Path

import streamlit as st

logger = logging.getLogger(__name__)

_UI_SUBDIR = "ui"

# Subdirectories within the storage
_CHATS_SUBDIR = "chats"
_CACHE_SUBDIR = "cache"
_SETTINGS_FILE = "settings.yaml"


def get_storage_base_path() -> Path:
    """Get the storage path for UI settings and chats.

    Returns:
        Path to <databao_project_path>/ui/
    """
    databao_project = st.session_state.databao_project
    if databao_project is None:
        raise RuntimeError("Databao project path not set")
    return databao_project.databao_dir / _UI_SUBDIR


def get_settings_path() -> Path:
    """Get the path to settings.yaml file.

    Returns:
        Path to settings.yaml
    """
    base = get_storage_base_path()
    return base / _SETTINGS_FILE


def get_chats_dir() -> Path:
    """Get the chats directory path.

    Returns:
        Path to chats directory
    """
    base = get_storage_base_path()
    chats_dir = base / _CHATS_SUBDIR
    chats_dir.mkdir(parents=True, exist_ok=True)
    return chats_dir


def get_cache_dir() -> Path:
    """Get the cache directory path for DiskCache.

    Returns:
        Path to cache directory
    """
    base = get_storage_base_path()
    cache_dir = base / _CACHE_SUBDIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def get_chat_dir(chat_id: str) -> Path:
    """Get the directory for a specific chat.

    Args:
        chat_id: The chat's unique ID

    Returns:
        Path to the chat's directory
    """
    chats = get_chats_dir()
    chat_dir = chats / chat_id
    chat_dir.mkdir(parents=True, exist_ok=True)
    return chat_dir
