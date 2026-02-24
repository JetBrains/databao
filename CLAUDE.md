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
   - Extract branch prefixes from existing remote branches, filtering out system prefixes (dependabot, revert-*, HEAD)
   - Use the most common matching prefix as the user's nickname
   - If no existing branches found, ask the user for their nickname
   - Branch format: `<nickname>/<descriptive-branch-name>`

   Strategy:
   ```bash
   # Extract and count branch prefixes, excluding system prefixes
   git branch -r | sed -nE 's|^ *origin/([^/]+)/.*|\1|p' | grep -vE '^(dependabot|HEAD|revert-)' | sort | uniq -c | sort -rn
   ```

3. **Create a separate branch** with the appropriate prefix (never commit directly to main)

4. **Commit the changes** with clear, descriptive commit messages

5. **Create a Pull Request** with the following format:

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
