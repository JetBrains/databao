"""Custom build hook for building the frontend multimodal."""

import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Build hook to compile the frontend during package build."""

    def initialize(self, version: str, build_data: dict) -> None:
        """Build the frontend multimodal before packaging.

        This hook ensures that the template.html file is built from the
        React frontend before the package is created.
        """
        root = Path(self.root)
        template_path = root / "databao" / "multimodal" / "template.html"
        client_dir = root / "client" / "multimodal"

        if not client_dir.exists():
            print("Warning: client/ directory not found. Skipping frontend build.", file=sys.stderr)
            print("The html() method will not work without the built frontend.", file=sys.stderr)
            return

        print("Building frontend...", file=sys.stderr)

        if not shutil.which("pnpm"):
            print("Error: pnpm not found. Please install pnpm to build the frontend.", file=sys.stderr)
            print("You can install it with: npm install -g pnpm", file=sys.stderr)
            raise FileNotFoundError("pnpm not found in PATH")

        try:
            subprocess.run(
                ["pnpm", "install"],
                cwd=client_dir,
                check=True,
                capture_output=True,
            )

            result = subprocess.run(
                ["pnpm", "run", "build"],
                cwd=client_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            if result.stdout:
                print(result.stdout, file=sys.stderr)

            if result.stderr:
                print(result.stderr, file=sys.stderr)

            index_path = root / "databao" / "multimodal" / "index.html"
            if index_path.exists():
                index_path.rename(template_path)
                print(f"Frontend built successfully: {template_path}", file=sys.stderr)
            else:
                print(f"Warning: Built index.html not found at {index_path}", file=sys.stderr)

        except subprocess.CalledProcessError as e:
            print(f"Error building frontend: {e}", file=sys.stderr)
            print("stdout:", e.stdout.decode() if e.stdout else "", file=sys.stderr)
            print("stderr:", e.stderr.decode() if e.stderr else "", file=sys.stderr)
            raise
        except FileNotFoundError:
            print("Error: pnpm not found. Please install pnpm to build the frontend.", file=sys.stderr)
            print("You can install it with: npm install -g pnpm", file=sys.stderr)
            raise
