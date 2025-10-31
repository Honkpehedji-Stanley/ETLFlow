# ETLFlow

ETLFlow is a simple ETL pipeline example orchestrated with Apache Airflow and running with Docker Compose.

It demonstrates a pipeline that extracts data from a CSV, transforms it with pandas, and loads it into PostgreSQL.

## Structure

```
etlflow/
├── dags/
│   └── etl_pipeline.py
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── data/
│   └── input_data.csv
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Quick start (using Docker Compose)

1. Install Docker & Docker Compose.
2. From the project root, start the stack:

```bash
docker compose up --build
```

3. Initialize Airflow metadata (first run):

```bash
# in another terminal
docker compose exec airflow-webserver airflow db init
docker compose exec airflow-webserver airflow users create \
    --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

4. Open Airflow UI at http://localhost:8080, unpause the `etl_pipeline` DAG and trigger it.

Note: the `docker-compose.yml` included is a minimal demo. See Airflow official Docker Compose docs for production-ready setups.

## Scripts

- `scripts/extract.py`: reads `data/input_data.csv` and writes `data/extracted.parquet`.
- `scripts/transform.py`: reads extracted data, cleans/renames/enriches and writes `data/transformed.parquet`.
- `scripts/load.py`: reads transformed data and upserts to PostgreSQL (env-driven connection).

## Environment variables for DB (used by load script)

Set these for the Postgres connection (the docker compose provides these values by default):

- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_DB
- POSTGRES_HOST
- POSTGRES_PORT

## Local dependencies

If you want to run the scripts locally (outside Airflow container), install:

```bash
pip install -r requirements.txt
```

## License

MIT
