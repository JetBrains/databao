#!/usr/bin/env python3
"""
Third-Party License Report Generator

This script generates a comprehensive license report for all third-party dependencies
used in this project, covering both Python and JavaScript/Node.js packages.

Usage:
    python generate_licenses.py              # Generate license report

Requirements:
    - Python 3.12+
    - uv (Python package manager)
    - pnpm (JavaScript package manager)
    - pip-licenses (installed automatically via uv --with flag)

Output File:
    - databao-agent-third-party-list.csv - Combined CSV report with all dependencies

The CSV includes a "Source" column to identify whether each package is from
Python or JavaScript. Intermediate files are created during generation but
automatically cleaned up.
"""

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], cwd: Path | None = None, description: str = "") -> bool:
    """Run a shell command and return success status."""
    if description:
        print(f"📦 {description}...")

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        return False


def generate_python_licenses(output_file: Path, no_confirm: bool = False) -> bool:
    """
    Generate Python license report using uv and pip-licenses.

    This command:
    1. Syncs production dependencies only (--no-dev)
    2. Runs pip-licenses in the project environment (uv run --with pip-licenses)
    3. Generates a CSV with package names, versions, licenses, authors, and URLs
    4. Filters out the project itself from the final output (see read_python_licenses)

    Note: We can't use --no-install-project or uv tool run because:
    - uv run --with reinstalls the project regardless of --no-install-project
    - uv tool run runs in isolation and can't see the synced dependencies
    - Solution: Install everything including the project, then filter it out in post-processing

    Args:
        output_file: Path to write the CSV output
        no_confirm: Skip confirmation prompt if True
    """
    # Warn user about environment modification
    if not no_confirm:
        print("⚠️  WARNING: This will modify your current Python environment!")
        print("   The environment will be synced to PRODUCTION dependencies only.")
        print("   - Development dependencies will be removed (--no-dev)")
        print("   You'll need to run 'uv sync' after this to restore dev dependencies.")
        print()
        response = input("Do you want to proceed? [y/N]: ").strip().lower()

        if response not in ("y", "yes"):
            print("❌ Cancelled by user.")
            return False
        print()

    cmd = [
        "uv",
        "sync",
        "--no-dev",
    ]

    # First sync dependencies
    if not run_command(cmd, description="Syncing Python production dependencies"):
        return False

    # Then generate license report
    cmd = [
        "uv",
        "run",
        "--with",
        "pip-licenses",
        "pip-licenses",
        "--format=csv",
        "--with-urls",
        "--with-authors",
        f"--output-file={output_file}",
    ]

    return run_command(cmd, description=f"Generating Python licenses to {output_file}")


def generate_javascript_licenses(output_file: Path) -> bool:
    """
    Generate JavaScript license report using pnpm.

    This uses pnpm's built-in 'licenses ls' command with:
    - --prod: Production dependencies only (excludes devDependencies)
    - --json: JSON format output
    """
    # Script may be in scripts/ or root, find project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent if script_dir.name == "scripts" else script_dir
    client_dir = project_root / "client"

    if not client_dir.exists():
        print(f"❌ Error: Client directory not found at {client_dir}", file=sys.stderr)
        return False

    cmd = ["pnpm", "licenses", "ls", "--prod", "--json"]

    print(f"📦 Generating JavaScript licenses to {output_file}...")

    try:
        result = subprocess.run(
            cmd,
            cwd=client_dir,
            check=True,
            capture_output=True,
            text=True,
        )

        # Write output to file
        output_file.write_text(result.stdout)
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        if e.stderr:
            print(e.stderr, file=sys.stderr)
        return False


def read_python_licenses(file_path: Path) -> list[dict[str, str]]:
    """
    Read and parse Python licenses from CSV format.

    Filters out the current project itself (databao) from the report since it's not
    a third-party dependency. The project gets installed during 'uv run --with'
    even though we only want third-party packages in the final report.
    """
    licenses = []

    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filter out the project itself - only include actual third-party dependencies
            if row["Name"].lower() == "databao":
                continue

            licenses.append(
                {
                    "Name": row["Name"],
                    "Version": row["Version"],
                    "License": row["License"],
                    "Author": row["Author"],
                    "URL": row["URL"],
                    "Source": "Python",
                }
            )

    return licenses


def read_javascript_licenses(file_path: Path) -> list[dict[str, str]]:
    """
    Read and parse JavaScript licenses from pnpm JSON format.

    pnpm outputs licenses grouped by license type, with each package containing:
    - name: Package name
    - versions: List of versions (we take the first)
    - license: License identifier
    - author: Package author
    - homepage: Package homepage/URL
    """
    licenses = []

    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    # Iterate through license types and packages
    for license_type, packages in data.items():
        for package in packages:
            # Handle multiple versions - take the first one
            version = package["versions"][0] if package["versions"] else "unknown"
            author = package.get("author", "UNKNOWN")
            homepage = package.get("homepage", "")

            licenses.append(
                {
                    "Name": package["name"],
                    "Version": version,
                    "License": license_type,
                    "Author": author,
                    "URL": homepage,
                    "Source": "JavaScript",
                }
            )

    return licenses


def combine_licenses(
    python_file: Path,
    js_file: Path,
    output_file: Path,
) -> bool:
    """
    Combine Python and JavaScript license reports into a single CSV.

    The combined CSV includes all packages sorted alphabetically by name,
    with a "Source" column indicating whether each package is from Python
    or JavaScript.
    """
    print("🔄 Combining license reports...")

    try:
        # Read both license files
        python_licenses = read_python_licenses(python_file)
        print(f"  ✓ Read {len(python_licenses)} Python packages")

        js_licenses = read_javascript_licenses(js_file)
        print(f"  ✓ Read {len(js_licenses)} JavaScript packages")

        # Combine and sort by name (case-insensitive)
        all_licenses = python_licenses + js_licenses
        all_licenses.sort(key=lambda x: x["Name"].lower())

        # Write combined CSV
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            fieldnames = ["Name", "Version", "License", "Author", "URL", "Source"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_licenses)

        print(f"✅ Successfully created {output_file}")
        print(f"   Total: {len(all_licenses)} packages")
        print(f"   - Python: {len(python_licenses)}")
        print(f"   - JavaScript: {len(js_licenses)}")

        return True

    except Exception as e:
        print(f"❌ Error combining licenses: {e}", file=sys.stderr)
        return False


def main() -> int:
    """Main entry point for the license report generator."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-y",
        "--yes",
        "--no-confirm",
        dest="no_confirm",
        action="store_true",
        help="Skip confirmation prompts (auto-accept environment modifications)",
    )

    args = parser.parse_args()

    # Determine paths
    # Script is in scripts/, but outputs go to project root
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent if script_dir.name == "scripts" else script_dir

    # Temporary intermediate files
    python_file = base_dir / ".databao-agent-third-party-list-python.csv"
    js_file = base_dir / ".databao-agent-third-party-list-js.json"

    # Final output file
    output_file = base_dir / "databao-agent-third-party-list.csv"

    try:
        # Generate Python licenses to temp file
        if not generate_python_licenses(python_file, no_confirm=args.no_confirm):
            return 1

        # Generate JavaScript licenses to temp file
        if not generate_javascript_licenses(js_file):
            return 1

        # Combine into final output file
        if not combine_licenses(python_file, js_file, output_file):
            return 1

        print("\n🎉 License report generation complete!")
        print(f"📄 Output: {output_file}")
        return 0

    finally:
        # Clean up temporary files
        for temp_file in [python_file, js_file]:
            if temp_file.exists():
                temp_file.unlink()
                print(f"🧹 Cleaned up temporary file: {temp_file.name}")


if __name__ == "__main__":
    sys.exit(main())
