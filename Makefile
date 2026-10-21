.PHONY: install db migrate test test-unit lint typecheck run demo bench bench-quick

install:
	uv sync --extra otel

db:
	docker compose -f deployments/docker-compose.yml up -d --wait postgres

migrate: db
	uv run flowforge migrate

test: db
	uv run pytest -q

test-unit:
	uv run pytest -q tests/unit

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run mypy

run:
	scripts/run-local.sh

demo:
	scripts/demo.sh

bench: db
	uv run python scripts/benchmark.py

bench-quick: db
	uv run python scripts/benchmark.py --quick
