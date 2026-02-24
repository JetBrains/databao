#!/usr/bin/env python3
"""Build and publish databao to the JetBrains dev PyPI.

Usage:
    python scripts/publish_to_dev_pypi.py

Environment variables (required):
    PYPI_DEV_USERNAME  - JetBrains Space username
    PYPI_DEV_PASSWORD  - JetBrains Space personal token

Prerequisites:
    - databao-context-engine must be published to the dev PyPI first
    - pnpm must be installed (for frontend build)

To bump the version, edit `version` in pyproject.toml (e.g. 0.1.4.dev1 -> 0.1.4.dev2).
"""

import hashlib
import os
import subprocess
import sys
import urllib.request
import zipfile
from base64 import b64encode
from email.parser import Parser
from io import BytesIO
from pathlib import Path

REPO_URL = "https://packages.jetbrains.team/pypi/p/ai-debugger/pypi-databao-dev/legacy"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_credentials() -> tuple[str, str]:
    username = os.environ.get("PYPI_DEV_USERNAME")
    password = os.environ.get("PYPI_DEV_PASSWORD")
    if not username or not password:
        print("Error: Set PYPI_DEV_USERNAME and PYPI_DEV_PASSWORD environment variables.")
        sys.exit(1)
    return username, password


def get_wheel_metadata(whl_path: Path) -> dict[str, str]:
    with zipfile.ZipFile(whl_path) as z:
        for name in z.namelist():
            if name.endswith("/METADATA"):
                return Parser().parsestr(z.read(name).decode())
    raise RuntimeError(f"No METADATA found in {whl_path}")


def upload_file(fpath: Path, metadata, username: str, password: str) -> bool:
    print(f"  Uploading {fpath.name}...")
    file_data = fpath.read_bytes()
    md5 = hashlib.md5(file_data).hexdigest()
    sha256 = hashlib.sha256(file_data).hexdigest()

    filetype = "bdist_wheel" if fpath.suffix == ".whl" else "sdist"
    pyversion = "py3" if fpath.suffix == ".whl" else ""

    boundary = "----PublishBoundary9f3c4d2a"
    fields = [
        (":action", "file_upload"),
        ("protocol_version", "1"),
        ("metadata_version", metadata.get("Metadata-Version", "2.1")),
        ("name", metadata.get("Name", "")),
        ("version", metadata.get("Version", "")),
        ("summary", metadata.get("Summary", "")),
        ("home_page", metadata.get("Home-page", "")),
        ("author", metadata.get("Author", "")),
        ("author_email", metadata.get("Author-email", "")),
        ("license", metadata.get("License", "")),
        ("description", metadata.get_payload() or ""),
        ("keywords", metadata.get("Keywords", "")),
        ("classifiers", ""),
        ("platform", ""),
        ("md5_digest", md5),
        ("sha256_digest", sha256),
        ("filetype", filetype),
        ("pyversion", pyversion),
        ("requires_python", metadata.get("Requires-Python", "")),
    ]

    body = BytesIO()
    for key, val in fields:
        body.write(f"--{boundary}\r\n".encode())
        body.write(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        body.write(f"{val}\r\n".encode())

    body.write(f"--{boundary}\r\n".encode())
    body.write(f'Content-Disposition: form-data; name="content"; filename="{fpath.name}"\r\n'.encode())
    body.write(b"Content-Type: application/octet-stream\r\n\r\n")
    body.write(file_data)
    body.write(f"\r\n--{boundary}--\r\n".encode())

    auth = b64encode(f"{username}:{password}".encode()).decode()
    req = urllib.request.Request(REPO_URL, data=body.getvalue(), method="POST")
    req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

    try:
        resp = urllib.request.urlopen(req)
        print(f"    {resp.status} - OK")
        return True
    except urllib.error.HTTPError as e:
        print(f"    {e.code} {e.reason}: {e.read().decode()}")
        return False


def main():
    username, password = get_credentials()

    print("Building package (includes frontend build)...")
    dist_dir = PROJECT_ROOT / "dist"
    subprocess.run(["rm", "-rf", str(dist_dir)], check=True)
    result = subprocess.run(["uv", "build"], cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Build failed:\n{result.stderr}")
        sys.exit(1)
    print(result.stderr.strip())

    whl = next(dist_dir.glob("*.whl"))
    sdist = next(dist_dir.glob("*.tar.gz"))
    metadata = get_wheel_metadata(whl)

    version = metadata.get("Version")
    name = metadata.get("Name")
    print(f"\nPublishing {name} {version} to {REPO_URL}")

    ok = upload_file(whl, metadata, username, password)
    ok = upload_file(sdist, metadata, username, password) and ok

    if ok:
        print(f"\nDone! Published {name}=={version}")
    else:
        print("\nSome uploads failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
