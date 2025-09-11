# 🐍 Банк задач для собеседований по Python

В этом разделе собраны практические задачи на Python, которые проверяют умение работать с данными и оптимизацией.  

---

## Задача: Выгрузка данных из PostgreSQL в CSV файл

Необходимо написать код на Python, который будет выгружать данные из таблицы PostgreSQL `big_table` и записывать их в CSV файл.  
Особенность: таблица содержит **40 миллионов строк**, поэтому данные нужно выгружать чанками, чтобы избежать переполнения памяти.

<details>
<summary>Показать решение</summary>

```python
import psycopg2
import csv

dsn = "host=... dbname=... user=... password=... port=5432"
outfile = "big_table.csv"
chunk_size = 100_000

with psycopg2.connect(dsn) as conn:
    # серверный курсор — результат не буферизуется целиком
    cur = conn.cursor(name="ss_cur")
    cur.itersize = chunk_size
    cur.execute("SELECT * FROM public.big_table ORDER BY id")  # важно иметь порядок/индекс

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # заголовки
        colnames = [desc[0] for desc in cur.description]
        writer.writerow(colnames)

        rows_written = 0
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            writer.writerows(rows)
            rows_written += len(rows)
            # print(f"written: {rows_written}")
```
</details>

---

## Задача: Перенос данных из PostgreSQL в ClickHouse

Напишите код на Python, который переносит данные из таблицы PostgreSQL `clients` в таблицу ClickHouse `clients`.  
Данные выгружаются чанками, чтобы избежать переполнения памяти.

<details>
<summary>Показать решение</summary>

```python
import psycopg2
import clickhouse_connect

PG_DSN = "host=... dbname=... user=... password=... port=5432"
CH_HOST = "http://localhost:8123"   # или http://ch-host:8123
CH_USER = "default"
CH_PASS = ""
CH_DB   = "default"

CHUNK = 100_000

# 1) Читаем из PostgreSQL чанками
with psycopg2.connect(PG_DSN) as pg_conn:
    pg_cur = pg_conn.cursor(name="ss_clients")  # server-side курсор
    pg_cur.itersize = CHUNK
    pg_cur.execute("SELECT id, name, email, created_at FROM public.clients ORDER BY id")

    # 2) Подключаемся к ClickHouse
    ch = clickhouse_connect.get_client(host=CH_HOST, username=CH_USER, password=CH_PASS, database=CH_DB)
    # Опционально: создаём таблицу при отсутствии
    ch.query("""
        CREATE TABLE IF NOT EXISTS clients (
            id UInt64,
            name String,
            email String,
            created_at DateTime
        ) ENGINE = MergeTree
        ORDER BY id
    """)

    # 3) Гоним чанками
    total = 0
    columns = ["id", "name", "email", "created_at"]

    while True:
        rows = pg_cur.fetchmany(CHUNK)
        if not rows:
            break

        # Вставка пачкой
        ch.insert("clients", rows, column_names=columns)
        total += len(rows)
        # print(f"Inserted: {total}")
```
</details>