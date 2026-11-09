# RBK Airflow: витрина продаж

Учебный проект для загрузки данных из CSV в PostgreSQL и обновления витрины продаж с помощью Apache Airflow.

## Метрика

` sales_volume ` — сумма `count * price_sale_out` по дате ревизии и магазину из таблицы `bd_shops.product`.

SQL-функция `bd_shop_dm.refresh_dm_sales()` обновляет таблицу `bd_shop_dm.dm_sales`. Критический порог — 250 000 на магазин за дату. Если значение выше порога, функция формирует ошибку с датой, магазином, суммой и лимитом. Airflow отмечает задачу как неуспешную и сохраняет сообщение в логах.

## DAG

DAG `rbk_sales_mart_pipeline` состоит из трёх последовательных задач:

1. `load_csv_to_market_size` — загружает строки из CSV в `bd_shops.market_size`.
2. `refresh_sales_mart` — вызывает SQL-функцию обновления витрины.
3. `log_success` — записывает сообщение об успешном выполнении.

Расписание: каждый понедельник в 09:00, часовой пояс `Europe/Moscow`.

## Файлы проекта

- `dags/rbk_sales_dag.py` — код DAG.
- `dags/data/store_sizes.csv` — дополнительные данные, 10 строк.
- `refresh_dm_sales.sql` — SQL-функция.
- `screenshots/` — скриншоты структуры DAG и логов.

## Подключение

В Airflow создано подключение PostgreSQL с ID `rbk_postgres`. Учебная база восстановлена из архива `productstar-team/RBK_Airflow`.

Пароли и другие секреты в репозиторий не включены.