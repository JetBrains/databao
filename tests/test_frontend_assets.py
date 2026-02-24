"""Tests that verify frontend assets are present and correctly bundled in the package.

Builds an actual wheel with `hatch build` and inspects the zip archive to confirm
that `force-include` in pyproject.toml correctly embeds every frontend asset.
This is the only test that catches a misconfigured pyproject.toml / hatch_build.py.

Run packaging tests with:
    pytest -m packaging tests/test_frontend_assets.py
"""

import subprocess
import zipfile
from pathlib import Path

import pytest

_REQUIRED_WHEEL_PATHS = [
    "client/out/multimodal-html/index.html",
    "client/out/multimodal-jupyter/index.js",
    "client/out/multimodal-jupyter/style.css",
]

# ---------------------------------------------------------------------------
# Wheel-packaging check
# ---------------------------------------------------------------------------


@pytest.mark.packaging
def test_frontend_assets_are_included_in_wheel(tmp_path: Path) -> None:
    """Build an actual wheel and verify every frontend asset is inside it.

    This test catches a broken pyproject.toml
    force-include or a hatch_build.py regression that would cause pip-installed
    packages to be missing the frontend files at runtime.
    """
    repo_root = Path(__file__).parent.parent.resolve()

    result = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"`uv build` failed:\n{result.stdout}\n{result.stderr}"

    wheels = list(tmp_path.glob("*.whl"))
    assert len(wheels) == 1, f"Expected exactly one wheel, found: {wheels}"
    wheel = wheels[0]

    with zipfile.ZipFile(wheel) as zf:
        names = set(zf.namelist())

    missing = [p for p in _REQUIRED_WHEEL_PATHS if p not in names]
    assert not missing, (
        f"The following frontend assets are missing from {wheel.name}:\n"
        + "\n".join(f"  - {p}" for p in missing)
        + "\n\nCheck the `force-include` entries in pyproject.toml "
        "and the hatch_build.py hook."
    )
