"""Services for the Databao Streamlit app."""

from databao.ui.services.chat_persistence import (
    delete_all_chats,
    delete_chat,
    load_all_chats,
    load_chat,
    save_chat,
    save_current_chat,
)
from databao.ui.services.chat_title import (
    check_title_completion,
    trigger_title_generation,
)
from databao.ui.services.query_executor import (
    QueryResult,
    cancel_query,
    check_query_completion,
    is_query_running,
    start_query_execution,
)
from databao.ui.services.settings_persistence import (
    delete_settings,
    get_or_create_settings,
    load_settings,
    save_settings,
)
from databao.ui.services.storage import (
    get_cache_dir,
    get_chat_dir,
    get_chats_dir,
    get_settings_path,
    get_storage_base_path,
)

__all__ = [
    # Chat title
    "trigger_title_generation",
    "check_title_completion",
    # Query execution
    "QueryResult",
    "start_query_execution",
    "check_query_completion",
    "is_query_running",
    "cancel_query",
    # Chat persistence
    "save_chat",
    "save_current_chat",
    "load_chat",
    "load_all_chats",
    "delete_chat",
    "delete_all_chats",
    # Settings persistence
    "save_settings",
    "load_settings",
    "delete_settings",
    "get_or_create_settings",
    # Storage
    "get_storage_base_path",
    "get_settings_path",
    "get_chats_dir",
    "get_chat_dir",
    "get_cache_dir",
]
