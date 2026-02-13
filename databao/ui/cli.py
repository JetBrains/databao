import importlib
import subprocess
import sys


def _get_streamlit_app_path() -> str:
    """Get the path to the Streamlit app without importing it.

    This avoids triggering module-level Streamlit code during import.
    """
    spec = importlib.util.find_spec("databao.ui.app")
    if spec is None or spec.origin is None:
        raise ValueError("Could not find databao.ui.app module. Make sure databao[ui] is installed.")
    return spec.origin


def bootstrap_streamlit_app(project_path: str, streamlit_args: list[str] | None = None):
    """Bootstrap the UI."""

    if streamlit_args is None:
        streamlit_args = []

    app_path = _get_streamlit_app_path()

    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", app_path, *streamlit_args, "--", "-d", project_path],
        check=True,
    )
