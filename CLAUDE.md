# Claude Code Agent Instructions

## After Completing Work

When you finish implementing changes:

1. **Run pre-commit checks:**
   ```bash
   make check
   # or directly: uv run pre-commit run --all-files
   ```

2. **Run tests:**
   ```bash
   make test
   # or directly: uv run pytest -v
   ```

3. **After checks pass**, suggest to the user that you can help create a branch, commit changes, and create a pull request.

Wait for user confirmation before proceeding.

### If User Confirms

1. **Check if GitHub CLI is installed:**
   ```bash
   gh auth status
   ```

   If not installed or not authenticated, guide the user to [DEVELOPMENT.md](DEVELOPMENT.md) for setup instructions. They can also choose to create the PR manually.

2. **Determine the branch name prefix:**
   - First, extract branch prefixes from existing remote branches to find the user's nickname
   - Filter out system prefixes (dependabot, revert-*, HEAD)
   - If found, use the most common prefix; otherwise fall back to git username
   - Branch format: `<nickname>/<descriptive-branch-name>`

   Strategy:
   ```bash
   # Get current user email
   git config user.email

   # Extract and count branch prefixes (format: prefix/branch-name)
   git branch -r | grep -oP '(?<=origin/)[^/]+(?=/)' | sort | uniq -c | sort -rn

   # Verify found prefix by checking for existing branches
   git branch -r | grep "origin/<prefix>/"

   # Fallback: use simplified git username if no branches exist
   git config user.name | awk '{print tolower($1)}'
   ```

3. **Create a separate branch** with the appropriate prefix (never commit directly to main)

3. **Commit the changes** with clear, descriptive commit messages

4. **Create a Pull Request** with the following format:

### Pull Request Format

```markdown
## Summary
Brief overview of what was changed and why.

## Changes

### Change 1: [Feature/Fix Name]
Brief description of this specific change.

<details>
<summary>Affected files</summary>

- `path/to/file1.py`
- `path/to/file2.py`
- `path/to/file3.py`

</details>

### Change 2: [Another Feature/Fix Name]
Brief description of this specific change.

<details>
<summary>Affected files</summary>

- `path/to/file4.py`
- `path/to/file5.py`

</details>

## Test Plan
- How the changes were tested
- Any manual testing steps required
```

### Guidelines

- Each logical change should be its own section
- List all affected files under a collapsible `<details>` spoiler
- Keep descriptions clear and concise
- Include relevant context for reviewers
