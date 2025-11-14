# Publishing to PyPI

To publish a new release to PyPI:
1. `git tag -a vX.Y.Z -m vX.Y.Z` and `git push --tags` or use the GitHub UI.
2. The [publish.yml](.github/workflows/publish.yml) workflow will trigger when a new version tag is pushed.

The package version will be set automatically from the tag.
