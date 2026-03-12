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
            if file_path.suffix == ".yaml":
                self.context = self.convert_from_yaml(file_path)
            else:
                with open(file_path) as f:
                    self.context = json.load(f)
                    self._apply_exclude()

    def convert_from_yaml(self, file_path: Path) -> dict[str, Any]:
        with open(file_path) as f:
            d = yaml.safe_load(f)
        result = {"catalogs": {}}
        for catalog in d["context"]["catalogs"]:
            catalog_name = catalog["name"]
            result["catalogs"][catalog_name] = {"schemas": {}}
            for schema in catalog["schemas"]:
                schema_name = schema["name"]
                result["catalogs"][catalog_name]["schemas"][schema_name] = {"tables": {}}
                if "description" in schema and schema["description"]:
                    result["catalogs"][catalog_name]["schemas"][schema_name]["description"] = schema["description"]
                for table in schema["tables"]:
                    table_name = table["name"]
                    res_tables = result["catalogs"][catalog_name]["schemas"][schema_name]["tables"]
                    res_tables[table_name] = {"columns": {}}
                    if "description" in table and table["description"]:
                        res_tables[table_name]["description"] = table["description"]
                    for column in table["columns"]:
                        column_name = column["name"]
                        data = {k: v for k, v in column.items() if k != "name" and v}
                        res_tables[table_name]["columns"][column_name] = data

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
