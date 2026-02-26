# Research: databao Namespace & Packaging Strategy

## Context

Three related packages need a coherent packaging strategy:
- **databao-agent** (PyPI: `databao`) — the main SDK, published, already supports `from databao import Agent`
- **databao-cli** (not yet published) — should expose a `databao ask "..."` CLI command
- **databao-context** (PyPI: `databao-context`) — currently `from databao_context import Domain`

User referenced: https://packaging.python.org/en/latest/guides/packaging-namespace-packages

**Key insight first:** Three distinct things share the name `databao` but are independent:
- PyPI **package name** (`pip install databao`) — a distribution identifier
- Python **module name** (`import databao`) — what Python resolves at runtime
- CLI **command name** (`databao ask`) — a PATH script, installed via `[project.scripts]`

These can be mixed independently. A CLI entry point is just a PATH script — its Python module does NOT need to be named `databao`.

---

## Current State

```toml
# databao-agent pyproject.toml
[project]
name = "databao"
# No [project.scripts] — no CLI entry points defined

[tool.hatch.build.targets.wheel]
include = ["/databao"]   # ← module named databao with __init__.py
```

The `databao/__init__.py` exports: `Agent`, `Domain`, `DBConnection`, `LLMConfig`, etc.
`from databao import Agent` **already works today**.

---

## Option A: Minimal Change (Recommended near-term)

No namespace packages. Zero breaking changes.

### How it works
- `databao-agent` stays exactly as-is
- `databao-cli` has its Python module named `databao_cli`, but registers the `databao` CLI command via entry point:
  ```toml
  # databao-cli pyproject.toml
  [project]
  name = "databao-cli"
  dependencies = ["databao>=X.Y"]   # pulls in SDK automatically

  [project.scripts]
  databao = "databao_cli.main:app"  # installs `databao` to PATH
  ```
- `databao-context` stays as-is

### Installation & usage
```bash
pip install databao          # SDK only
pip install databao-cli      # CLI + SDK (databao-cli depends on databao)

from databao import Agent    # ✓
databao ask "question?"      # ✓
from databao_context import Domain  # ✓ (unchanged)
```

### Pros
- Zero breaking changes
- No coordination required between repos
- Simple, standard packaging
- CLI entry point name is completely independent of Python module name
- `databao-cli` depends on `databao`, so installing the CLI also installs the SDK

### Cons
- `from databao_context import Domain` remains underscore-style (not `databao.context`)
- Two separate import styles (`databao.*` vs `databao_context.*`)
- Not a unified namespace

---

## Option B: Native Namespace Packages (Clean long-term architecture)

Use PEP 420 implicit namespace packages. All packages contribute sub-modules under the `databao` top-level namespace.

### How it works
- **No package may have `databao/__init__.py`** — this is a hard requirement
- Each package occupies a sub-directory:
  ```
  databao-agent/src/databao/agent/    ← from databao.agent import Agent
  databao-cli/src/databao/cli/        ← from databao.cli import main
  databao-context/src/databao/context/ ← from databao.context import Domain
  ```
- CLI entry point still works the same way:
  ```toml
  [project.scripts]
  databao = "databao.cli.main:app"
  ```

### Installation & usage
```bash
pip install databao          # installs databao.agent sub-namespace
pip install databao-cli      # installs databao.cli + CLI command
pip install databao-context  # installs databao.context

from databao.agent import Agent    # ✓ (breaking change from current API!)
from databao.context import Domain # ✓
databao ask "question?"            # ✓
```

### Pros
- Clean, unified namespace
- Discoverable and professional (`help(databao)` shows all sub-packages)
- True separation of concerns per package
- Industry pattern (e.g., `google-cloud-*` packages use `google.cloud.*`)

### Cons
- **BREAKING**: `from databao import Agent` → must become `from databao.agent import Agent`
- All three repos must coordinate simultaneously — if any one adds `__init__.py` to `databao/`, the entire namespace breaks silently for users
- Requires removing `databao/__init__.py` from the current repo (major refactor)
- `pip install databao` would no longer give `from databao import Agent` directly (unless you add a compatibility shim package)
- Fragile: the `__init__.py` contamination problem (see below); pytest/mypy need extra config for namespace packages

### Installation order for Option B

**Good news:** For pure native namespace packages, installation order generally does NOT matter. Python's import machinery (PEP 420) scans all `sys.path` entries at import time, collects every directory named `databao/` that lacks an `__init__.py`, and merges them into a single namespace object on the fly. No file "wins" — they all contribute.

**The real risk — `__init__.py` contamination:**
The order problem only appears if any package accidentally ships a `databao/__init__.py`. When Python finds one:
1. It treats `databao` as a regular package (not a namespace) and stops scanning
2. All other packages' `databao/` subdirectories become invisible
3. `from databao.context import Domain` silently breaks with `ModuleNotFoundError`

Because this happens at import time based on `sys.path` order (not install time), the result is non-deterministic across environments. A user who installs packages in a different order, or reinstalls one package, may get different behavior.

**Other edge cases:**
- Two packages shipping the same file (e.g., both have `databao/utils.py`) — last-installed wins (pip overwrites)
- Editable installs (`pip install -e .`) — modern PEP 660 editable installs can interact unexpectedly with namespace packages; legacy editable installs (adding to `sys.path`) work fine
- Conda + pip mixing — conda and pip may resolve namespace directories differently

### Mitigation for breaking change
Could ship a transitional `databao/__init__.py` as a separate "compat" package that re-exports from `databao.agent`, but this requires a 4th package or a carefully ordered installation, and breaks the namespace requirement again.

---

## Option C: pkgutil-Style Namespace Packages (Legacy compatibility)

**Status: Deprecated. Not recommended for new projects.**

Allows `__init__.py` to exist in the namespace directory, with:
```python
__path__ = __import__('pkgutil').extend_path(__path__, __name__)
```

Could theoretically allow `from databao import Agent` (from the agent `__init__.py`) AND `from databao.context import Domain` (from context sub-package) simultaneously.

### Pros
- Backward-compatible imports could coexist
- More permissive than native namespace packages

### Cons
- Officially deprecated (PEP 451)
- Every package must include the magic `__init__.py` line — coordination required
- Behavior is subtle and fragile: whichever package is installed last "wins" for the `__init__.py` content
- Mixing with native namespace packages breaks everything
- Adds maintenance burden

**Not recommended.**

---

## Option D: Hybrid — Keep SDK as-is, Add Namespace for Context/CLI

A pragmatic middle path:
- `databao-agent` keeps its current flat `databao/__init__.py` with all exports intact
- `databao-context` adds a new `databao_context.context` module that re-exports as `from databao_context import Domain` still works, but also enables future `from databao.context import Domain` via a thin wrapper or rename
- `databao-cli` uses Option A (separate module, `databao` CLI command)

This defers the namespace decision and avoids breaking changes, but doesn't achieve full unification.

---

## Recommendation

### Short term: Option A (now)
1. **databao-agent**: No changes needed — `from databao import Agent` already works
2. **databao-cli**: Publish with module `databao_cli`, entry point `databao = "databao_cli:main"`. Declare `databao` as a dependency so installing the CLI also installs the SDK.
3. **databao-context**: No changes needed

### Long term: Option B (future major version)
When ready for a `v2.0` / breaking release:
1. Remove `databao/__init__.py` from all three repos
2. Move SDK code to `databao/agent/` (or keep at `databao/sdk/`)
3. Provide migration guide: `from databao import Agent` → `from databao.agent import Agent`
4. Optionally ship a compatibility shim via a `databao-compat` package

### For databao-context specifically (Option D flavor)
A light improvement with no breakage:
- `databao_context` stays as-is
- Add a `databao.context` alias package in the future that just re-exports from `databao_context`

---

## Verification

For Option A (implementation of databao-cli):
1. `pip install databao-cli` in a fresh venv
2. Confirm `databao` is auto-installed as a dependency
3. `databao ask "what tables do I have?"` runs
4. `from databao import Agent` still works in the same venv
5. `from databao_cli import ...` works for programmatic CLI use

---

## References
- https://packaging.python.org/en/latest/guides/packaging-namespace-packages
- https://peps.python.org/pep-0420/ (native namespace packages)
- https://setuptools.pypa.io/en/latest/userguide/package_discovery.html
