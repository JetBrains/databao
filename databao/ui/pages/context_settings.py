"""Context Settings page - DCE project configuration."""

import streamlit as st

from databao.dce import DCEProject, DCEProjectStatus
from databao.ui.app import _clear_all_chat_threads
from databao.ui.components.sidebar import get_db_icon
from databao.ui.components.status import AppStatus, set_status


def render_context_settings_page() -> None:
    """Render the Context Settings page."""
    st.title("Context Settings")
    st.markdown("Configure your data context and sources.")

    st.markdown("---")

    # Current project section
    st.subheader("📊 DCE Project")

    project: DCEProject | None = st.session_state.get("databao_project")
    reload_clicked = False

    if project is not None:
        reload_clicked = _render_project_info(project)
    else:
        st.info("No DCE project detected. Configure one below.")

    # Handle reload
    if reload_clicked:
        # Clear project object but keep databao_project_path so it reloads from same location
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
        elif project.status == DCEProjectStatus.NO_BUILD:
            st.warning("Project needs to be built first. Run `nemory build`.")
        else:
            st.caption("Sources will appear after initialization.")
    else:
        _render_sources(agent)


def _render_project_info(project: DCEProject) -> bool:
    """
    Render project information.
    
    Returns:
        True if the project was reloaded, False otherwise.
    """
    st.markdown(f"**{project.name}**")
    st.code(str(project.path), language=None)

    # Status indicator
    if project.status == DCEProjectStatus.VALID:
        st.success("Project is ready", icon="✅")
        if project.latest_run:
            st.caption(f"Latest build: {project.latest_run}")
    elif project.status == DCEProjectStatus.NO_BUILD:
        st.warning("Build required - run `nemory build`", icon="⚠️")
    else:
        st.error("Project not found", icon="❌")

    reload_clicked = st.button("🔄 Reload")

    return reload_clicked


def _render_sources(agent) -> None:
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
            db_type = type(conn).__name__

            if "duckdb" in db_type.lower():
                icon = get_db_icon("duckdb")
                db_type = "DuckDB"
            elif "engine" in db_type.lower():
                try:
                    dialect = conn.dialect.name
                    icon = get_db_icon(dialect)
                    db_type = dialect.capitalize()
                except Exception:
                    icon = get_db_icon("default")
                    db_type = "Database"
            else:
                icon = get_db_icon("default")

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
