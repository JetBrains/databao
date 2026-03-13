import json
from pathlib import Path
from typing import Any, Union, cast

Node = dict[str, Union["Node", str, int]]


class Observer:
    def __init__(
        self,
        file_path: Path | None = None,
        exclude: list[str] | None = None,
    ):
        self.context: Node = {}
        self._exclude = exclude or []
        if file_path:
            with open(file_path) as f:
                data = json.load(f)
            if "nodes" in data:
                self.context = self.convert_from_manifest(data)
            else:
                self.context = data
                self._apply_exclude()

    def convert_from_manifest(self, data: dict[str, Any]) -> Node:
        result: Node = {"catalogs": {}}
        for node in data.get("nodes", {}).values():
            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                continue
            database = node.get("database", "")
            schema = node.get("schema", "")
            table = node.get("name", "")
            if not (database and schema and table):
                continue

            catalogs: Node = cast(Node, result["catalogs"])
            if database not in catalogs:
                catalogs[database] = {"schemas": {}}
            db_node: Node = cast(Node, catalogs[database])
            schemas: Node = cast(Node, db_node["schemas"])
            if schema not in schemas:
                schemas[schema] = {"tables": {}}
            tables: Node = cast(Node, schemas[schema])

            table_entry: dict[str, Any] = {"columns": {}}
            description = node.get("description", "")
            if description:
                table_entry["description"] = description
            for col_name, col in node.get("columns", {}).items():
                col_data = {k: v for k, v in col.items() if k != "name" and v}
                table_entry["columns"][col_name] = col_data

            tables[table] = cast(Node, table_entry)

        return result

    def _apply_exclude(self) -> None:
        if not self._exclude:
            return

        for t in cast(Node, self.context.get("tables", {})).values():
            t_node = cast(Node, t)
            for c in cast(Node, t_node.get("columns", {})).values():
                c_node = cast(Node, c)
                if "common_sqls" in c_node:
                    common_sqls = cast(Node, c_node["common_sqls"])
                    to_del = [k for k, v in common_sqls.items() if v in self._exclude]
                    for k in to_del:
                        del common_sqls[k]

    def get_node(self, path: list[str], depth: int = 0) -> str:
        if self.context is None:
            return "Data is not available."
        if not path:
            answer = "Result for empty path - root:\n"
            res: str | Node | int = self.context
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

    def get_value(self, path: list[str], current_node: Node) -> str | Node | int:
        if path[0] not in current_node:
            return f"Node {path[0]} not found."
        value = current_node[path[0]]
        if len(path) == 1:
            return value
        if not isinstance(value, dict):
            return f"Node {path[0]} is not a dict."
        return self.get_value(path[1:], value)

    def get_depth_1(self, node: Node) -> str:
        res = ""
        for k, v in node.items():
            if isinstance(v, dict):
                res += f"{k}: {{ ({len(v)}) }}\n"
            else:
                res += f"{k}: {v!s}\n"
        return res.strip()

    def get_auto_depth(self, node: Node) -> str:
        depth_1 = self.get_depth_1(node)
        depth_2 = self.get_with_depth(node, 2)
        if len(depth_2) < 1000:
            return depth_2.strip()
        return depth_1.strip()

    def get_with_depth(self, node: Node, depth: int) -> str:
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
