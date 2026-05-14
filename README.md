# Classicmodels MySQL to PostgreSQL Star Schema Pipeline


```text
MySQL classicmodels
  -> Scheduled Spark ETL 
  -> PostgreSQL star_schema_dw (Data Warehouse)
      -> Schema: star_schema (Dimensions & Facts)
```

Stack:

- `mysql`: source RDBMS, seed database `classicmodels`.
- `postgresql`: data warehouse for star schema
- `etl`: the main ETL process
- `query`: querying (to check if star schema is correct)
- `airflow`: monitor, orchestrate pipeline

## Running

```bash
docker compose --profile airflow up
```
Access Airflow UI at: http://localhost:8080 (User/Password: admin/admin).
Query results:

```bash
docker compose run --rm query
```

## DAG
```text
classicmodels_mysql_to_postgres_star_schema```

DAG flow:
```text
start
  -> check_mysql_ready
  -> check_dw_ready
  -> prepare_dw_schema
  -> run_spark_etl
  -> validate_star_schema
  -> end
```
![Airflow UI](image.png)

Scheduling:

- DAG scheduled with `schedule=timedelta(minutes=1)`.
- `max_active_runs=1` to prevent multiple runs at the same time.

Orchestration:

- Airflow quan ly thu tu task.
- If MySQL/PostgreSQL check failed, ETL can't execute.
- If ETL failed, validation won't run.

Monitoring:

- Airflow UI display Graph, Grid, history, task state, Gantt chart, logs and other details
![check_mysql_ready logs](image-1.png)

