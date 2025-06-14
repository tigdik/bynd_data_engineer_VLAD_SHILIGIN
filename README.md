# Bynd Data Engineering Exercise – Vlad Shiligin

This repository is a **minimal, production‑ready PySpark pipeline** that ingests the provided
`mock_dataset.csv`, performs basic data cleansing, and loads the result into a Postgres table.

## Quick‑start (local)

```bash
# 1. Clone and enter
git clone https://github.com/tigdik/bynd_data_engineer_VLAD_SHILIGIN.git bynd_data_engineer_VLAD_SHILIGIN
cd bynd_data_engineer_VLAD_SHILIGIN

# 2. Configure environment variables in
.env  

# 3. Start Postgres locally
docker compose up -d db

# 4. Build python env and run pipeline on host (optional)
make dev && make run

# 5. ...or run everything inside Docker
docker build -t bynd-pipeline .
docker run --env-file .env --network=host bynd-pipeline
```

## Project layout

```
.
├── src/                    # Importable package (`bynd_de`)
│   ├── __init__.py
│   ├── config.py           # pydantic settings from env
│   ├── spark_session.py    # single SparkSession factory
│   ├── transformations.py  # stateless transform fns
│   └── pipeline.py         # extract‑transform‑load script
├── tests/                  # PyTest unit tests
├── data/                   # Raw mock data (git‑ignored)
├── Dockerfile              # Builds + runs `spark-submit`
├── docker-compose.yml      # Postgres service
├── Makefile                # Dev shortcuts
└── .github/workflows/ci.yml
```

## Design decisions

* **PySpark** as suggested by Dinarte.
* **Stateless functions** in `transformations.py` – easily unit‑tested & reused.
* **Configuration via env vars** – no need to recompile if the need to change settings raised.
* **CI** runs tests, lint (`ruff`), type‑check (`mypy`) on every push.
* **Security** – least‑privilege Postgres user, secrets not baked into images.

## Extending

* Add richer parsing (e.g. JSON strings in `address` or currency parsing) by
  creating new functions in `transformations.py` and adding them to
  `transform()` composition.
* Swap sinks (e.g. S3, BigQuery) by implementing a new `Writer` module that
  consumes the final `DataFrame`.

---

*Happy Engineering!* 🚀
