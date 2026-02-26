# Development Guidelines

## Recommended Tools

### GitHub CLI

GitHub CLI (gh) is recommended for streamlined pull request creation and repository management.

**Check if installed and authenticated:**
```bash
gh auth status
```

**If not installed, install GitHub CLI:**
- macOS: `brew install gh`
- Linux: See [official installation guide](https://github.com/cli/cli/blob/trunk/docs/install_linux.md)
- Windows: `winget install --id GitHub.cli`

**Authenticate:**
```bash
gh auth login
```

Follow the prompts to authenticate with your GitHub account.

## YouTrack MCP Integration

The YouTrack MCP server gives Claude Code direct access to YouTrack — searching issues, creating tickets, adding comments, logging work, and more.

### Setup

**1. Generate a permanent token:**

Go to [JetBrains Hub → Authentication](https://hub.jetbrains.com/users/me?tab=authentication), create a new token with **YouTrack** scope, and copy it.

**2. Add the MCP server to Claude Code:**

```bash
claude mcp add --transport http --scope user youtrack https://youtrack.jetbrains.com/mcp \
  --header "Authorization: Bearer <your-token>"
```

The `--scope user` flag makes it available across all projects.

**3. Restart Claude Code** to pick up the new MCP server.

### Verify

Ask Claude: *"Who am I in YouTrack?"* — it should respond with your name and email via `get_current_user`.

## Git Workflow

### Branch Naming Convention

Branch names should follow the pattern: `<nickname>/<descriptive-branch-name>`

Where:

- `<nickname>` is your personal short identifier — if you don't have one yet, pick something short, distinctive, and branch-name compatible (lowercase, no spaces, e.g. `jsmith`, `alex`, `kosta`)
- `<descriptive-branch-name>` briefly describes the change (e.g., `fix-auth-bug`, `add-plotting-feature`)

Examples:

- `jsmith/fix-connection-timeout`
- `alex/add-mysql-support`
- `kosta/update-readme`

## Code Quality

### Enable Pre-commit Hooks

To automatically run checks on every commit, install the pre-commit hooks:

```bash
uv run pre-commit install
```

This will run linting and formatting checks automatically before each commit.

To run checks manually on all files:
```bash
make check
# or: uv run pre-commit run --all-files
```

### Running Tests

Run the test suite:
```bash
make test
# or: uv run pytest -v
```

To run only tests that don't require API keys:
```bash
uv run pytest -v -m "not apikey"
```
