clean: clean-common

clean-common:
	@echo "Cleaning up node_modules..."
	@find . -name "node_modules" -type d -prune -exec rm -rf "{}" +
	@echo "Cleaning up dist..."
	@find . -name "dist" -type d -prune -exec rm -rf "{}" +
	@echo "Cleaning up build..."
	@find . -name 'build' -type d -prune -exec rm -rf '{}' +
	@echo "Cleaning up out..."
	@rm -rf out
	@echo "Cleaning up nx..."
	@rm -rf .nx
