"""Services for the Databao Streamlit app."""

from databao.ui.services.chat_persistence import (
    delete_all_chats,
    delete_chat,
    load_all_chats,
    load_chat,
    save_chat,
    save_current_chat,
)
from databao.ui.services.chat_title import check_title_completion, trigger_title_generation
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
    "QueryResult",
    "cancel_query",
    "check_query_completion",
    "check_title_completion",
    "delete_all_chats",
    "delete_chat",
    "delete_settings",
    "get_cache_dir",
    "get_chat_dir",
    "get_chats_dir",
    "get_or_create_settings",
    "get_settings_path",
    "get_storage_base_path",
    "is_query_running",
    "load_all_chats",
    "load_chat",
    "load_settings",
    "save_chat",
    "save_current_chat",
    "save_settings",
    "start_query_execution",
    "trigger_title_generation",
]
