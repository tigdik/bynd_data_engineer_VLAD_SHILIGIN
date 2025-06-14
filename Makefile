.PHONY: dev up down run test lint format

dev:
    python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

up:
    docker compose up -d db

down:
    docker compose down -v

run:
    . .venv/bin/activate && spark-submit src/pipeline.py

test:
    . .venv/bin/activate && pytest -q

lint:
    . .venv/bin/activate && ruff src tests && mypy src

format:
    . .venv/bin/activate && ruff format src tests
