"""Chat persistence service for saving and loading chat sessions."""

from __future__ import annotations

import json
import logging
import pickle
import shutil
from typing import TYPE_CHECKING, Any

from databao.ui.services.storage import get_cache_dir, get_chat_dir, get_chats_dir

if TYPE_CHECKING:
    from databao.ui.models.chat_session import ChatSession

logger = logging.getLogger(__name__)


def save_current_chat() -> None:
    """Save the current chat from session state to disk.

    This is a convenience function that gets the current chat from
    st.session_state and saves it using save_chat().
    """
    import streamlit as st

    current_chat_id = st.session_state.get("current_chat_id")
    chats = st.session_state.get("chats", {})
    if current_chat_id and current_chat_id in chats:
        save_chat(chats[current_chat_id])

# File names within chat directory
_SESSION_FILE = "session.json"
_RESULTS_DIR = "results"
_VISUALIZATIONS_DIR = "visualizations"


def save_chat(chat: ChatSession) -> None:
    """Save a chat session to disk.

    Saves:
    - session.json: Chat metadata and messages (without results)
    - results/{idx}.pkl: Pickled ExecutionResult for each message that has one

    Args:
        chat: The ChatSession to save
    """
    chat_dir = get_chat_dir(chat.id)

    # Save session JSON
    session_data = chat.to_dict()
    session_path = chat_dir / _SESSION_FILE
    with open(session_path, "w") as f:
        json.dump(session_data, f, indent=2)

    # Save results as pickle files
    results_dir = chat_dir / _RESULTS_DIR
    results_dir.mkdir(parents=True, exist_ok=True)

    for i, msg in enumerate(chat.messages):
        result_path = results_dir / f"{i}.pkl"
        if msg.result is not None:
            try:
                with open(result_path, "wb") as f:
                    pickle.dump(msg.result, f)
            except Exception as e:
                logger.warning(f"Failed to pickle result for message {i}: {e}")
                # Remove failed pickle file if it exists
                if result_path.exists():
                    result_path.unlink()
        elif result_path.exists():
            # Clean up old result file if message no longer has result
            result_path.unlink()

    # Save visualization spec_df as pickle files (spec is in session.json via visualization_data)
    visualizations_dir = chat_dir / _VISUALIZATIONS_DIR
    visualizations_dir.mkdir(parents=True, exist_ok=True)

    for i, msg in enumerate(chat.messages):
        vis_df_path = visualizations_dir / f"{i}_spec_df.pkl"
        vis_data = msg.visualization_data
        if vis_data is not None and vis_data.get("spec_df") is not None:
            try:
                with open(vis_df_path, "wb") as f:
                    pickle.dump(vis_data["spec_df"], f)
            except Exception as e:
                logger.warning(f"Failed to pickle visualization spec_df for message {i}: {e}")
                if vis_df_path.exists():
                    vis_df_path.unlink()
        elif vis_df_path.exists():
            # Clean up old file if visualization was removed
            vis_df_path.unlink()

    logger.debug(f"Chat saved: {chat.id}")


def load_chat(chat_id: str) -> ChatSession | None:
    """Load a chat session from disk.

    Args:
        chat_id: The chat's unique ID

    Returns:
        ChatSession if found, None otherwise
    """
    from databao.ui.models.chat_session import ChatSession

    chats_dir = get_chats_dir()
    chat_dir = chats_dir / chat_id

    if not chat_dir.exists():
        return None

    session_path = chat_dir / _SESSION_FILE
    if not session_path.exists():
        return None

    try:
        # Load session JSON
        with open(session_path) as f:
            session_data = json.load(f)

        # Load results
        results_dir = chat_dir / _RESULTS_DIR
        results: list[Any] = []

        num_messages = len(session_data.get("messages", []))
        for i in range(num_messages):
            result_path = results_dir / f"{i}.pkl"
            if result_path.exists():
                try:
                    with open(result_path, "rb") as f:
                        results.append(pickle.load(f))
                except Exception as e:
                    logger.warning(f"Failed to unpickle result {i} for chat {chat_id}: {e}")
                    results.append(None)
            else:
                results.append(None)

        # Load visualization spec_df into session_data
        visualizations_dir = chat_dir / _VISUALIZATIONS_DIR
        for i, msg_data in enumerate(session_data.get("messages", [])):
            vis_data = msg_data.get("visualization_data")
            if vis_data is not None:
                vis_df_path = visualizations_dir / f"{i}_spec_df.pkl"
                if vis_df_path.exists():
                    try:
                        with open(vis_df_path, "rb") as f:
                            vis_data["spec_df"] = pickle.load(f)
                    except Exception as e:
                        logger.warning(f"Failed to unpickle visualization spec_df {i} for chat {chat_id}: {e}")

        # Create ChatSession with results
        chat = ChatSession.from_dict(session_data, results)
        logger.debug(f"Chat loaded: {chat_id}")
        return chat

    except Exception as e:
        logger.error(f"Failed to load chat {chat_id}: {e}")
        return None


def load_all_chats() -> dict[str, ChatSession]:
    """Load all chat sessions from disk.

    Returns:
        Dictionary mapping chat ID to ChatSession
    """
    chats_dir = get_chats_dir()
    chats: dict[str, ChatSession] = {}

    if not chats_dir.exists():
        return chats

    for chat_dir in chats_dir.iterdir():
        if not chat_dir.is_dir():
            continue

        chat_id = chat_dir.name
        chat = load_chat(chat_id)
        if chat is not None:
            chats[chat_id] = chat

    logger.info(f"Loaded {len(chats)} chats from disk")
    return chats


def delete_chat(chat_id: str) -> bool:
    """Delete a chat session and its cache data.

    Args:
        chat_id: The chat's unique ID

    Returns:
        True if deleted successfully, False otherwise
    """
    # Load chat to get cache_scope before deleting
    chat = load_chat(chat_id)
    cache_scope = chat.cache_scope if chat else None

    # Delete chat directory
    chats_dir = get_chats_dir()
    chat_dir = chats_dir / chat_id

    try:
        if chat_dir.exists():
            shutil.rmtree(chat_dir)
            logger.info(f"Deleted chat directory: {chat_dir}")
    except Exception as e:
        logger.error(f"Failed to delete chat directory {chat_dir}: {e}")
        return False

    # Delete cache scope data if available
    if cache_scope:
        try:
            _delete_cache_scope(cache_scope)
        except Exception as e:
            logger.warning(f"Failed to delete cache scope {cache_scope}: {e}")
            # Don't fail the whole operation if cache cleanup fails

    return True


def _delete_cache_scope(cache_scope: str) -> None:
    """Delete cache data for a specific scope.

    Args:
        cache_scope: The cache scope identifier (e.g., "agent_name/uuid")
    """
    from databao.caches.disk_cache import DiskCache, DiskCacheConfig

    cache_dir = get_cache_dir()
    config = DiskCacheConfig(db_dir=cache_dir / "diskcache")
    cache = DiskCache(config=config)

    try:
        # The cache scope is used as a tag prefix, we can evict by tag
        # Note: This may not perfectly clean up all scoped data depending on
        # how the cache is used, but it's a best effort cleanup
        cache.invalidate_tag(cache_scope)
        logger.debug(f"Invalidated cache tag: {cache_scope}")
    finally:
        cache.close()


def delete_all_chats() -> int:
    """Delete all chat sessions.

    Returns:
        Number of chats deleted
    """
    chats_dir = get_chats_dir()
    deleted = 0

    if not chats_dir.exists():
        return 0

    for chat_dir in list(chats_dir.iterdir()):
        if not chat_dir.is_dir():
            continue

        chat_id = chat_dir.name
        if delete_chat(chat_id):
            deleted += 1

    # Also clear the entire cache directory for a clean slate
    cache_dir = get_cache_dir()
    diskcache_dir = cache_dir / "diskcache"
    if diskcache_dir.exists():
        try:
            shutil.rmtree(diskcache_dir)
            logger.info(f"Cleared cache directory: {diskcache_dir}")
        except Exception as e:
            logger.warning(f"Failed to clear cache directory: {e}")

    logger.info(f"Deleted {deleted} chats")
    return deleted
