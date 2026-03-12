import json
import yaml
from pathlib import Path
from typing import Any


class Observer:
    def __init__(
        self,
        file_path: Path | None = None,
        exclude: list[str] | None = None,
    ):
        self.context: dict[str, Any] = {}
        self._exclude = exclude or []
        if file_path:
            with open(file_path) as f:
                data = json.load(f)
            if "nodes" in data:
                self.context = self.convert_from_manifest(data)
            else:
                self.context = data
                self._apply_exclude()

    def convert_from_manifest(self, data: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {"catalogs": {}}
        for node in data.get("nodes", {}).values():
            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                continue
            database = node.get("database", "")
            schema = node.get("schema", "")
            table = node.get("name", "")
            if not (database and schema and table):
                continue

            catalogs = result["catalogs"]
            if database not in catalogs:
                catalogs[database] = {"schemas": {}}
            schemas = catalogs[database]["schemas"]
            if schema not in schemas:
                schemas[schema] = {"tables": {}}
            tables = schemas[schema]["tables"]

            table_entry: dict[str, Any] = {"columns": {}}
            description = node.get("description", "")
            if description:
                table_entry["description"] = description
            for col_name, col in node.get("columns", {}).items():
                col_data = {k: v for k, v in col.items() if k != "name" and v}
                table_entry["columns"][col_name] = col_data

            tables[table] = table_entry

        return result

    def _apply_exclude(self) -> None:
        if not self._exclude:
            return

        for t in self.context.get("tables", {}).values():
            for c in t.get("columns", {}).values():
                if "common_sqls" in c:
                    to_del = [k for k, v in c["common_sqls"].items() if v in self._exclude]
                    for k in to_del:
                        del c["common_sqls"][k]

    def get_node(self, path: list[str], depth: int = 0) -> str:
        if self.context is None:
            return "Data is not available."
        if not path:
            answer = "Result for empty path - root:\n"
            res: str | dict[str, Any] = self.context
        else:
            res = self.get_value(path, self.context)
            answer = f"Result for path: {'/'.join(path)}, depth {depth}\n"
        if res:
            if isinstance(res, dict):
                if depth == 0:
                    return answer + self.get_auto_depth(res)
                return answer + self.get_with_depth(res, depth)
            else:
                return answer + str(res)
        return "Path not found."

    def get_value(self, path: list[str], current_node: dict) -> str | dict:
        if path[0] not in current_node:
            return f"Node {path[0]} not found."
        if len(path) == 1:
            return current_node[path[0]]
        return self.get_value(path[1:], current_node[path[0]])

    def get_depth_1(self, node: dict) -> str:
        res = ""
        for k, v in node.items():
            if isinstance(v, dict):
                res += f"{k}: {{ ({len(v)}) }}\n"
            else:
                res += f"{k}: {v!s}\n"
        return res.strip()

    def get_auto_depth(self, node: dict) -> str:
        depth_1 = self.get_depth_1(node)
        depth_2 = self.get_with_depth(node, 2)
        if len(depth_2) < 1000:
            return depth_2.strip()
        return depth_1.strip()

    def get_with_depth(self, node: dict, depth: int) -> str:
        if depth == 1:
            return self.get_depth_1(node)
        result = ""
        for k, v in node.items():
            if isinstance(v, dict):
                lines = self.get_with_depth(v, depth - 1).split("\n")
                result += f"{k}: {{\n"
                for line in lines:
                    result += f"  {line}\n"
                result += "}\n"
            else:
                result += f"{k}: {v!s}\n"

        return result.strip()
