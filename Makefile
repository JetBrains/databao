LATEST_STABLE := $(shell git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$$' | head -1)
CURRENT_MAJOR := $(shell echo $(LATEST_STABLE) | sed 's/^v//' | cut -d. -f1)
CURRENT_MINOR := $(shell echo $(LATEST_STABLE) | sed 's/^v//' | cut -d. -f2)
CURRENT_BUILD := $(shell echo $(LATEST_STABLE) | sed 's/^v//' | cut -d. -f3)

check:
	uv run pre-commit run --all-files

test:
	bash -c 'set -a; [ -f .env ] && source .env; set +a; uv run pytest tests/ -v'

release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Usage: make release VERSION=0.3.0"; \
		exit 1; \
	fi
	@echo "Creating release v$(VERSION)..."
	git tag -a "v$(VERSION)" -m "v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "Tag v$(VERSION) pushed. CI will publish to PyPI."

minor-release: VERSION = $(CURRENT_MAJOR).$(CURRENT_MINOR).$(shell echo $$(($(CURRENT_BUILD) + 1)))
minor-release: release

major-release: VERSION = $(CURRENT_MAJOR).$(shell echo $$(($(CURRENT_MINOR) + 1))).0
major-release: release

dev-release:
	gh workflow run publish-dev.yml
	@echo "Dev release workflow triggered."
