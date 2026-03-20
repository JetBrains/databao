from pathlib import Path

MEMORY_FOLDERS = ("general", "metrics", "vocabulary", "common_sql")
MEMORIES_INDEX = Path("memories/MEMORIES.md")

EMPTY_INDEX = "# Memory Index\n\n| Name | Path |\n|------|------|\n"


class MemoryManager:
    def __init__(self, project_path: Path, max_memories: int = 20):
        self.project_path = project_path
        self.max_memories = max_memories
        self.index_path = project_path / MEMORIES_INDEX
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.index_path.exists():
            self.index_path.write_text(EMPTY_INDEX)

    def read_index(self) -> str:
        return self.index_path.read_text()

    def _read_entries(self) -> list[dict[str, str]]:
        entries = []
        for line in self.index_path.read_text().splitlines():
            if line.startswith("|") and not line.startswith("| Name") and not line.startswith("|---"):
                parts = [p.strip() for p in line.split("|")[1:-1]]
                if len(parts) == 2:
                    entries.append({"name": parts[0], "path": parts[1]})
        return entries

    def _write_entries(self, entries: list[dict[str, str]]) -> None:
        rows = "\n".join(f"| {e['name']} | {e['path']} |" for e in entries)
        self.index_path.write_text(
            "# Memory Index\n\n| Name | Path |\n|------|------|\n" + (rows + "\n" if rows else "")
        )

    def count(self) -> int:
        return len(self._read_entries())

    def add(self, name: str, folder: str, filename: str, content: str) -> str:
        if folder not in MEMORY_FOLDERS:
            return f"Error: folder must be one of {MEMORY_FOLDERS}."
        entries = self._read_entries()
        if any(e["name"] == name for e in entries):
            return f"Error: memory '{name}' already exists. Use update_memory to modify it."
        if len(entries) >= self.max_memories:
            return (
                f"WARNING: Memory is full ({len(entries)}/{self.max_memories}). "
                "Delete irrelevant memories with delete_memory before adding new ones. "
                "Only the most important facts should be kept."
            )
        try:
            path = self._safe_resolve(f"memories/{folder}/{filename}")
        except ValueError as exc:
            return f"Error: {exc}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        entries.append({"name": name, "path": f"memories/{folder}/{filename}"})
        self._write_entries(entries)
        return f"Memory '{name}' saved to memories/{folder}/{filename}."

    def _safe_resolve(self, raw_path: str) -> Path:
        memories_root = (self.project_path / "memories").resolve()
        resolved = (self.project_path / raw_path).resolve()
        if not str(resolved).startswith(str(memories_root) + "/"):
            raise ValueError(f"Path '{raw_path}' escapes the memories directory.")
        return resolved

    def delete(self, name: str) -> str:
        entries = self._read_entries()
        entry = next((e for e in entries if e["name"] == name), None)
        if not entry:
            return f"Error: memory '{name}' not found."
        try:
            self._safe_resolve(entry["path"]).unlink(missing_ok=True)
        except ValueError as exc:
            return f"Error: {exc}"
        self._write_entries([e for e in entries if e["name"] != name])
        return f"Memory '{name}' deleted."

    def update(self, name: str, content: str) -> str:
        entries = self._read_entries()
        entry = next((e for e in entries if e["name"] == name), None)
        if not entry:
            return f"Error: memory '{name}' not found. Use add_memory to create it."
        try:
            self._safe_resolve(entry["path"]).write_text(content)
        except ValueError as exc:
            return f"Error: {exc}"
        return f"Memory '{name}' updated."
