import csv
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONNECTION_ID = "rbk_postgres"
CSV_PATH = "/opt/airflow/dags/data/store_sizes.csv"


def load_store_sizes():
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    if len(rows) < 10:
        raise ValueError(f"В CSV должно быть не менее 10 строк, найдено: {len(rows)}")

    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)

    for row in rows:
        name = row["size_name"].strip()
        min_square = int(row["min_square"])

        if not name or min_square < 0:
            raise ValueError(f"Некорректная строка CSV: {row}")

        # Обновляем строку по названию, чтобы повторный запуск не создавал дубль.
        hook.run(
            "DELETE FROM bd_shops.market_size WHERE size_name = %s",
            parameters=(name,),
        )
        hook.run(
            "INSERT INTO bd_shops.market_size (size_name, min_square) "
            "VALUES (%s, %s)",
            parameters=(name, min_square),
        )

    logging.info("Загружено строк из CSV: %s", len(rows))


def refresh_sales_mart():
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    hook.run("SELECT bd_shop_dm.refresh_dm_sales();")


def log_success():
    logging.info("DAG rbk_sales_mart_pipeline выполнен успешно")


with DAG(
    dag_id="rbk_sales_mart_pipeline",
    description="Загрузка CSV и обновление витрины продаж",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"),
    schedule="0 9 * * 1",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["rbk", "training"],
) as dag:
    load_csv = PythonOperator(
        task_id="load_csv_to_market_size",
        python_callable=load_store_sizes,
    )

    refresh_mart = PythonOperator(
        task_id="refresh_sales_mart",
        python_callable=refresh_sales_mart,
    )

    success = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    load_csv >> refresh_mart >> success