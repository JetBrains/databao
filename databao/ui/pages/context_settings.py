"""Context Settings page - DCE project configuration."""

import streamlit as st

from databao import Agent
from databao.ui.app import _clear_all_chat_threads
from databao.ui.components.sidebar import get_db_icon
from databao.ui.components.status import AppStatus, set_status
from databao.ui.databao_project import DatabaoProject, DCEProjectStatus


def render_context_settings_page() -> None:
    """Render the Context Settings page."""
    st.title("Context Settings")
    st.markdown("Configure your data context and sources.")

    st.markdown("---")

    # Current project section
    st.subheader("📊 DCE Project")

    project: DatabaoProject | None = st.session_state.get("databao_project")
    reload_clicked = False

    if project is not None:
        reload_clicked = _render_project_info(project)
    else:
        st.info("No DCE project detected. Configure one below.")

    # Handle reload
    if reload_clicked:
        st.session_state.databao_project = None
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Reloading project...")
        st.rerun()

    st.markdown("---")

    # Connected sources section
    st.subheader("🔗 Connected Sources")

    agent = st.session_state.get("agent")
    if agent is None:
        if project is None:
            st.caption("Configure a project to see available sources.")
        elif project.dce_status == DCEProjectStatus.NO_BUILD:
            st.warning("Project needs to be built first. Run `nemory build`.")
        else:
            st.caption("Sources will appear after initialization.")
    else:
        _render_sources(agent)


def _render_project_info(project: DatabaoProject) -> bool:
    """
    Render project information.
    
    Returns:
        True if the project was reloaded, False otherwise.
    """
    st.markdown(f"**{project.name}**")
    st.code(str(project.path), language=None)

    # Status indicator
    if project.dce_status == DCEProjectStatus.VALID:
        st.success("Project is ready", icon="✅")
    elif project.dce_status == DCEProjectStatus.NO_BUILD:
        st.warning("Build required - run `nemory build`", icon="⚠️")
    else:
        st.error("Project not found", icon="❌")

    reload_clicked = st.button("🔄 Reload")

    return reload_clicked


def _render_sources(agent: Agent) -> None:
    """Render connected data sources."""
    dbs = agent.dbs
    dfs = agent.dfs

    if not dbs and not dfs:
        st.caption("No sources configured in this project.")
        return

    # Databases
    if dbs:
        st.markdown("**Databases:**")
        for name, source in dbs.items():
            conn = source.db_connection
            from databao.databases import DBConnectionConfig

            if isinstance(conn, DBConnectionConfig):
                # DBConnectionConfig - get type from config
                db_type_str = conn.type.full_type
                icon = get_db_icon(db_type_str)
                db_type = db_type_str.capitalize()
            elif hasattr(conn, "dialect"):
                # SQLAlchemy Engine/Connection
                try:
                    dialect = conn.dialect.name
                    icon = get_db_icon(dialect)
                    db_type = dialect.capitalize()
                except Exception:
                    icon = get_db_icon("default")
                    db_type = "Database"
            elif "duckdb" in type(conn).__name__.lower():
                icon = get_db_icon("duckdb")
                db_type = "DuckDB"
            else:
                icon = get_db_icon("default")
                db_type = "Database"

            with st.container():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"{icon} **{name}**")
                with col2:
                    st.caption(db_type)

                # Show context preview if available
                if source.context:
                    with st.expander("View context", expanded=False):
                        st.code(source.context[:500] + "..." if len(source.context) > 500 else source.context)

    # DataFrames
    if dfs:
        st.markdown("**DataFrames:**")
        for name in dfs:
            st.markdown(f"📊 **{name}**")
