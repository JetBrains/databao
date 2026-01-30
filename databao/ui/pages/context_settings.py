"""Context Settings page - DCE project configuration."""

from pathlib import Path

import streamlit as st

from databao.dce import DCEProject, DCEProjectStatus, find_best_project
from databao.dce.project import validate_project
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

    project: DCEProject | None = st.session_state.get("dce_project")

    if project is not None:
        _render_project_info(project)
    else:
        st.info("No DCE project detected. Configure one below.")

    st.markdown("---")

    # Project path selector
    st.subheader("🔧 Configure Project Path")

    # Use stored path if available, otherwise fall back to project path
    stored_path = st.session_state.get("dce_project_path")
    current_path = stored_path or (str(project.path) if project else "")
    custom_path = st.text_input(
        "Project path",
        value=current_path,
        placeholder="/path/to/your/nemory-project",
        help="Enter the path to a directory containing nemory.ini",
    )

    col1, col2 = st.columns(2)

    with col1:
        detect_clicked = st.button("🔍 Auto-detect", use_container_width=True)

    with col2:
        reload_clicked = st.button("🔄 Reload", use_container_width=True)

    # Handle auto-detect
    if detect_clicked:
        detected = find_best_project()
        if detected:
            st.session_state.dce_project = detected
            st.session_state.dce_project_path = str(detected.path)  # Store for persistence
            # Reset agent to reinitialize with new project
            st.session_state.agent = None
            _clear_all_chat_threads()
            set_status(AppStatus.INITIALIZING, "Loading detected project...")
            st.success(f"Found project at: {detected.path}")
            st.rerun()
        else:
            st.warning("No DCE project found in current directory or parent directories.")

    # Handle reload
    if reload_clicked:
        # Clear project object but keep dce_project_path so it reloads from same location
        st.session_state.dce_project = None
        st.session_state.agent = None
        _clear_all_chat_threads()
        set_status(AppStatus.INITIALIZING, "Reloading project...")
        st.rerun()

    # Handle manual path input
    if custom_path and custom_path != current_path:
        path = Path(custom_path).expanduser().resolve()
        if path.is_dir():
            validated = validate_project(path)
            if validated.status != DCEProjectStatus.NOT_FOUND:
                if st.button("✓ Apply", type="primary"):
                    st.session_state.dce_project = validated
                    st.session_state.dce_project_path = str(path)  # Store for persistence
                    st.session_state.agent = None
                    _clear_all_chat_threads()
                    set_status(AppStatus.INITIALIZING, "Loading project...")
                    st.rerun()
            else:
                st.error(f"No DCE project found at {path}")
        elif custom_path:
            st.error(f"Path does not exist: {path}")

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



def _render_project_info(project: DCEProject) -> None:
    """Render project information."""
    st.markdown(f"**{project.name}**")
    st.code(str(project.path), language=None)

    # Status indicator
    if project.status == DCEProjectStatus.VALID:
        st.success("✓ Project is ready", icon="✅")
        if project.latest_run:
            st.caption(f"Latest build: {project.latest_run}")
    elif project.status == DCEProjectStatus.NO_BUILD:
        st.warning("⚠️ Build required - run `nemory build`", icon="⚠️")
    else:
        st.error("❌ Project not found", icon="❌")


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
