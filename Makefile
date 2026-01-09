check:
	uv run pre-commit run --all-files

test:
	bash -c 'set -a; [ -f .env ] && source .env; set +a; uv run pytest tests/ -v'

build-client:
	cd client/multimodal && pnpm install && pnpm run build
	mv databao/multimodal/index.html databao/multimodal/template.html

dev-client:
	cd client/multimodal && pnpm run dev
