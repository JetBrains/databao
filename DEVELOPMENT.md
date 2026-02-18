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
