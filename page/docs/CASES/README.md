# Реальные задачи на работе


### КЕЙС - Витрина данных

**Витрина данных** – это просто табличка, которая уже сто раз отфильтрована, где-то очищена от лишних данных. Но у витрины данных могут быть разные источники. Например, у меня витрина собиралась из данных по дебетовым картам, кредитным картам, ипотекам, вкладам и так далее. Реально много разных источников-таблиц. Каждый источник обновляет у себя данные в разное время. У кого-то данные готовы за вчера, а где-то только за позавчера. 

Поэтому если мы будет запускать расчет витрины с одной датой за вчера (Т-1), то по каким-то источникам мы прочитаем свежие данные, а по каким-то будет 0 строк (данные в источнике еще не добавились). Че делать?

Я начал логировать в отдельную таблицу последнюю дату загрузки данных по каждому продукту во время расчета витрины. И при следующем запуске, мой скрипт ходит в таблицу с метаданными и читает для каждого продукта (карты, кредиты, вклады и т.д.) свою максимальную дату.

| Название продукта     | Данные загружены |
|-----------------------|---------------|
| Дебетовые карты       | 2024-11-10    |
| Кредитные карты       | 2024-11-11    |
| Ипотека               | 2024-11-15    |
| Вклады                | 2024-11-11    |
| Переводы СНГ         | 2024-11-18    |


Очевидно, что искать максимальную дату в такой таблице это очень быстро. Ну вот мы видим, что по дебетовым картам крайняя дата загрузки 2024-11-10. Следовательно, в следующий раз данные начнут грузиться за 11 ноября. А по Ипотеке уже с 16 ноября. При этом дата, до которой скрипту надо грузиться тоже не с неба берется. Я также читаю дату загрузки у самого источника. И делаю это не в лоб. Ну источник может быть держать данные за 10 лет и там сотни Террабайт данных. Делаю это через чтение метаданных о партициях:

```python
source_table = spark.sql(f'SHOW PARTITIONS {TABLE}')\
                            .select(F.regexp_extract("partition", f"date_event=(.*)", 1).alias("date"))\
                            .select(F.max("date").alias("max_date"))
                            .first()["max_date"]
```

Можем представить, что источник по Вкладам сдох и таблица с вкладами новых данных не имеет. Через месяц ее починят и наш скрипт начнет с той даты, которая была последней в витрине данных. При этом другие продукты никак не пострадают. 

***
### КЕЙС - Использование dbt

Ко мне пришли аналитики. У них было три SQL скрипта длиной в 500-800 строк. В этих SQL было очень много операций CASE WHEN и дальше какое-то условие, типа
```sql
SELECT
    CASE 
        WHEN URL LIKE '%business%' THEN 'camp_business'
        WHEN URL LIKE '%business1%' THEN 'camp_business1'
        WHEN URL LIKE '%business2%' THEN 'camp_business2'
        ... (500 условий)
        ELSE 'NO'
    END
FROM TABLE
```
У них это все невозможно было читать, рефакторить, да и они там потом еще делали несколько UNION и он падал, когда это можно было и не делать. Ну просто хардкодом были написаны скрипты. 
Я взял dbt и написал модельку типа:
```sql
{{ config(
    materialized='incremental',
    engine='ReplicatedMergeTree',
    tags=['etea']
) }}


# Read dbt_marks
{% set sql %}
    select * from {{ ref('dbt_marks') }}
{% endset %}
{% set res = run_query(sql) %}



{% set exec_date = var('execution_date')|string %}
{{ log("exec_date: " ~ exec_date, True) }}



# Save to condition_list, result_list
{% if execute %}

    {% set condition_list = res.columns[0].values() %}
    {% set result_list = res.columns[1].values() %}

{% else %}

    {% set results_list = [] %}

{% endif %}



{% for condition, result in zip(condition_list, result_list) %}
    SELECT
        dateTime,
        CASE
            WHEN {{ condition }} THEN {{ result }}
            ELSE 'undefined'
        END as URL,
        counterUserIDHash,
        isPageView
    FROM {{ source('schema', 'your_table') }}
    WHERE dateTime::DATE = '{{ exec_date }}'
    AND isPageView = '1'
    AND URL not like '%stage%'
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
```
Тут можно увидеть много непонятного, но присмотритесь к главному SQL скрипту. Там видно, что в условия CASE WHEN подставляется некий шаблон. У меня есть цикл, внутри которого запускается SQL запрос с ОДНИМ CASE WHEN. В этом цикле в условия поочередно подставляются значения. Эти значения берутся из обычного txt файлика. 

Вот так выглядит файлик:
```
condition,result
URL like '%business/ecom/smm%','smm'
URL like '%business/ecom/all%','all'
URL like '%business/nbs/insales%','insales'
...
```

И теперь аналитики могут просто добавлять сколько угодно своих дополнительных условий в этот файлик, а dbt будет автоматически читать оттуда все строчки и подставлять в запрос и выполнять его!

И теперь вместо 800 строк SQL кода у нас есть простой SQL запрос и txt файлик.

***
### КЕЙС - Из старого Greenplum в новый Greenplum

Была ситуация - был старый кластер Greenplum. В нем были таблицы, которые надо было перенести в новый Greenplum. Перетаскивать можно разными способами.

Вот первый способ в лоб:
Берем Apache Spark, подключаемся по jdbc к Гринпламу, пишем запрос и читаем данные. НО! В случае чтения Спарком через JDBC у нас все данные льются через мастер-хост. То самое бутылочное горлышко. Здесь можно обмануть всех и читать по отдельности с каждого сегмента Greenplum. Например добавить фильтр WHERE gp_segment_id = 0(1, 2, 3, 4 и так далее). Тогда мы сильно разгрузим матсерхост.

Второй вариант через гринплам коннектор:
Вот тут уже можно читать и записывать данные параллельно с разных сегментов одновременно. Нет того самого бутылочного горлышка на стороне ГП. Но при таком коннекторе у меня читалась вся таблица целиком типа ```SELECT * FROM TABLE```. Не вариант. Я не смог найти ответ, как задавать именно конкретный запрос, используя этот коннектор. Идем дальше.

Третий вариант через EXTERNAL TABLE:
В самом Dbeaver в Greenplum создаем внешнюю таблицу в S3 с помощью PXF. Короче в гринплам создается типа ссылка на данные в S3 (прям указывается конкретный путь до папки). Запрос пишем в ГП, а данные лежат в S3. Но при этом чтение все, как будто пользуемся обычной таблицей. Супер! Значит в старом Гринпламе создаем внешнюю таблицу в S3. И в новом Гринпламе создаем внешнюю таблицу в S3 на ту же папку! Т.е. у нас два ГП читают из одного места.

**Пример кода на создание внешней таблицы в Greenplum**.
При этом данные сгружаются в S3 и их можно прям потрогать

**Запись из Greenplum в S3**
```sql
Drop EXTERNAL TABLE if exists pxf_write_paqruet_s3;
CREATE WRITABLE EXTERNAL TABLE pxf_write_paqruet_s3 (
 visit_id text,
 body text,
 meta__checkpoint text
)
LOCATION ('pxf://data-lake/test/?PROFILE=s3:parquet&accesskey=***&secretkey=***&endpoint=https://storage.yandexcloud.net')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
INSERT into pxf_write_paqruet_s3
select *
from stg2.table
where meta__checkpoint = '2024-10-01';
```

**Чтение из S3 в Greenplum**
```sql
Drop EXTERNAL TABLE if exists pxf_read_paqruet_s3;
CREATE READABLE EXTERNAL TABLE pxf_read_paqruet_s3 (
 visit_id text,
 body text,
 meta__checkpoint text
)
LOCATION ('pxf://data-lake/test/?PROFILE=s3:parquet&accesskey=***&secretkey=***&endpoint=https://storage.yandexcloud.net')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

select *
from pxf_read_paqruet_s3;
```

Собственно в новом ГП мы делаем тоже самое, но уже не ``` INSERT INTO ```, а просто читаем, типа ``` SELECT * FROM TABLE ```. Это займет конечно время, если таблица огромная, но это всяко легче и быстрее, чем два предыдущих способа.



***
### КЕЙС - Как увеличить скорость чтения данных в Greenplum

**Проблема:**

Есть таблица в Greenplum. Она много весит и долго читается. Нужно, чтобы не занимала место в GP, но при этом быстро читалась. Короче убить двух зайцев.

**Какие есть варианты решения:**

| Вариант решения | Плюсы | Минусы |
|----------------|-------|--------|
| **Сгрузить таблицу в S3 через PXF** | Освободим место в Greenplum | Очень долгое чтение |
| **Партицировать внутри Greenplum по бизнес дате** | Быстрое чтение | Таблица занимает место + метаданные + нужно писать код для автоматического создания партиций |
| **Сгрузить в Hybrid Storage (Yezzey) всю таблицу** | Экономия места в Greenplum, чтение быстрее, чем через PXF | Все еще медленнее в десятки раз, чем в GP |
| **Сгрузить в Yezzey уже партицированную таблицу** | Экономия места и быстрое чтение (как в GP) | Нужно автоматизировать создание партиций перед записью |

**Решение:**
- Партицировать таблицу по дате (поле stat_date)
- Сгрузить в Yandex Hybrid Storage (Yezzey) -> [исходный код](https://github.com/open-gpdb/yezzey)
- Сгрузить все предыдущие года в Yezzey, а актуальный год оставить в GP


*Этот Yezzey похож на выгрузку в S3, но с некоторыми нюансами. Вот [статья на Habr](https://habr.com/ru/companies/yandex_cloud_and_infra/articles/831780/)*

**Как это сделать:**

Создаем таблицу с партициями. Но не рекомендую создавать сразу на 100 лет вперед. Во-первых у GP есть ограничение на 32767 партиций на таблицу. А во-вторых выгружать в Yezzey целый год и больше будет долго (в это время может упасть кластер и тогда все нужно будет начинать сначала). Создали партиции под ближайший месяц -> загрузили туда данные -> сгрузили в гибридное хранилище. И дальше по аналогии.

*Важно заметить, что писать данные, не создав для них партиций, не получится (выпадет ошибка). Можно сделать default партицию, куда будут падать данные, для которых не существует партиций, но тогда в чем смысл разбиения по бизнес дате (либо ошибка в самой бизнес дате)*

### Скрипт для создания таблички

Заметьте, создаю на ближайший месяц. И таблица создается все еще внутри Greenplum
```sql
create table stg2.hello_world (
    stat_date date null,
    body text null,
    meta__checkpoint text null
)
with (
    appendonly = true,
    compresstype = zstd,
    compresslevel = 3,
    orientation = column
)
distributed randomly
PARTITION BY RANGE (stat_date) (
    START ('2024-01-01') INCLUSIVE
    END ('2024-02-01') EXCLUSIVE
    EVERY (INTERVAL '1 day')
);
```

### Загрузка данных

Поскольку моя изначальная таблица уже имела много ретро данных и занимала кучу места, я прочитал ее при помощи Spark и сложил по папкам в формате parquet в наш обычный S3. Это можно считать обычным backup. Сделал это я для того, чтобы грузить данные в уже партицированную табличку из S3 и не нагружать GP.

**Чтение из S3 я делал итеративно по папкам-годам-месяцам через PXF:**
```sql
Drop EXTERNAL TABLE if exists pxf_read_paqruet_s3_stats;
CREATE READABLE EXTERNAL TABLE pxf_read_paqruet_s3_stats (
    stat_date date null,
    body text null,
    meta__checkpoint text null
)
LOCATION ('pxf://data-lake/hello_world/year=2024/partition_date=2024-01-*/?PROFILE=s3:parquet&accesskey=***&secretkey=***&endpoint=https://storage.yandexcloud.net')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

select *
from pxf_read_paqruet_s3;

insert into stg2.hello_world
select * from pxf_read_paqruet_s3_stats;
```

### Загрузка в Yezzey
Каждый раз, когда я загружал данные за месяц, они сгружались в Hybrid Storage. Я делал это по 1-3 месяца за раз, но не более.
```sql
SELECT yezzey_define_offload_policy('stg2', 'hello_world');
```

### Скрипт для создания партциий

Это обычный цикл, который создает метаданные для таблицы. Условно подготавливает место, куда будут литься новые данные.
```sql
-- Создать партиции от ВКЛЮЧИТЕЛЬНО до НЕВКЛЮЧИТЕЛЬНО
DO $$ 
DECLARE 
    partition_start DATE := '2024-02-01';
    partition_end DATE := '2024-03-01';
    next_day DATE;
BEGIN
    WHILE partition_start < partition_end LOOP
        next_day := partition_start + INTERVAL '1 day';
        
        EXECUTE format(
            'ALTER TABLE stg2.hello_world 
            ADD PARTITION START (''%s'') END (''%s'');', 
            partition_start, next_day
        );
        partition_start := next_day;
    END LOOP;
END $$;
```

Итого, я загрузил таблицу в гибридное хранилище от 2020 до 2024 года включительно, а текущий 2025 год оставил внутри Greenplum. Таблица партицирована по дням. Сейчас общее кол-во партиций под 1800. Очевидно, что предел в 32767 партиций я никогда не достигну - это почти 90 лет. Но неизвестно, насколько ок будет greenplum с метаданными. С каждым днем их будет становится все больше. 


### Выводы
Ниже я привел пример и план запроса для каждого из кейсов:

### SQL запрос
```sql
explain analyze
select count(1)
from stg2.hello_world
where stat_date = '2025-02-10';
```

| **Хранилище** | **Партиции**  | **Cost**                   | **Время выполнения**  |
|-------------------------|--------------|---------------------------|------------------|
| **GP**                 | По дню       | 168.03..168.04           | 1.08 сек        |
| **Yezzey**             | По дню       | 56640.78..56640.79       | 4.47 сек        |
| **Yezzey**             | Без партиций | 64656610.72..64656610.73 | **3 мин 29 сек** |
| **PXF S3**             | Без партиций | 13503.01..13503.02       | **5 мин 38 сек** |

### Выводы:
- **GP с партициями по дню** – самый быстрый вариант (1.08 сек) с минимальным cost (168).  
- **Yezzey с партициями** выполняется за 4.47 сек, что значительно быстрее, чем без партиций.  
- **Yezzey без партиций** выполняется **в 47 раз дольше**, чем с партициями (3 мин 29 сек против 4.47 сек).  
- **PXF S3 без партиций** – самый медленный (5 мин 38 сек).

 
### Ниже привожу планы запросов для каждого из кейсов:

- Партиции по дню (Greenplum)
```sql
Aggregate  (cost=168.03..168.04 rows=1 width=8) (actual time=730.806..730.807 rows=1 loops=1)
  ->  Gather Motion 48:1  (slice1; segments: 48)  (cost=167.51..168.01 rows=1 width=8) (actual time=39.514..730.637 rows=48 loops=1)
        ->  Aggregate  (cost=167.51..167.52 rows=1 width=8) (actual time=328.150..328.150 rows=1 loops=1)
              ->  Append  (cost=0.00..167.50 rows=1 width=0) (actual time=0.089..345.690 rows=64948 loops=1)
                    ->  Seq Scan on hello_world_1_prt_r287043979  (cost=0.00..167.50 rows=1 width=0) (actual time=0.088..218.898 rows=64948 loops=1)
                          Filter: (stat_date = '2025-02-10'::date)
Planning time: 2996.883 ms
  (slice0)    Executor memory: 4918K bytes.
  (slice1)    Executor memory: 234K bytes avg x 48 workers, 234K bytes max (seg0).
Memory used:  24576kB
Optimizer: Postgres query optimizer
Execution time: 1082.391 ms
```

- Партиции по дню (Yezzey)
```sql
Aggregate  (cost=56640.78..56640.79 rows=1 width=8) (actual time=4293.035..4293.035 rows=1 loops=1)
  ->  Gather Motion 48:1  (slice1; segments: 48)  (cost=56640.26..56640.76 rows=1 width=8) (actual time=977.990..4292.599 rows=48 loops=1)
        ->  Aggregate  (cost=56640.26..56640.28 rows=1 width=8) (actual time=1033.459..1033.460 rows=1 loops=1)
              ->  Append  (cost=0.00..50203.89 rows=53637 width=0) (actual time=1280.037..1334.369 rows=54148 loops=1)
                    ->  Seq Scan on hello_world_1_prt_r798590680  (cost=0.00..50203.89 rows=53637 width=0) (actual time=1280.037..1326.521 rows=54148 loops=1)
                          Filter: (stat_date = '2024-02-10'::date)
Planning time: 2388.470 ms
  (slice0)    Executor memory: 4918K bytes.
  (slice1)    Executor memory: 234K bytes avg x 48 workers, 234K bytes max (seg0).
Memory used:  24576kB
Optimizer: Postgres query optimizer
Execution time: 4467.812 ms
```

- Нет партиций (Yezzey)
```sql
Aggregate  (cost=64656610.72..64656610.73 rows=1 width=8) (actual time=209189.974..209189.974 rows=1 loops=1)
  ->  Gather Motion 48:1  (slice1; segments: 48)  (cost=64656610.21..64656610.71 rows=1 width=8) (actual time=77770.652..209189.813 rows=48 loops=1)
        ->  Aggregate  (cost=64656610.21..64656610.22 rows=1 width=8) (actual time=135874.293..135874.293 rows=1 loops=1)
              ->  Seq Scan on hello_world_old  (cost=0.00..64651431.80 rows=43154 width=0) (actual time=694.990..82860.795 rows=54016 loops=1)
                    Filter: (stat_date = '2024-02-10'::text)
Planning time: 1.600 ms
  (slice0)    Executor memory: 266K bytes.
  (slice1)    Executor memory: 322K bytes avg x 48 workers, 322K bytes max (seg0).
Memory used:  24576kB
Optimizer: Postgres query optimizer
Execution time: 209223.460 ms
```

- Нет партиций (PXF S3)
```sql
Aggregate  (cost=13503.01..13503.02 rows=1 width=8) (actual time=338455.033..338455.033 rows=1 loops=1)
  ->  Gather Motion 48:1  (slice1; segments: 48)  (cost=13502.50..13503.00 rows=1 width=8) (actual time=250234.310..338454.862 rows=48 loops=1)
        ->  Aggregate  (cost=13502.50..13502.51 rows=1 width=8) (actual time=297374.062..297374.062 rows=1 loops=1)
              ->  External Scan on pxf_read_paqruet_s3_stats  (cost=0.00..13500.00 rows=21 width=0) (actual time=257059.696..306274.212 rows=159778 loops=1)
                    Filter: (stat_date = '2024-02-10'::date)
Planning time: 0.260 ms
  (slice0)    Executor memory: 239K bytes.
  (slice1)    Executor memory: 220K bytes avg x 48 workers, 266K bytes max (seg19).
Memory used:  24576kB
Optimizer: Postgres query optimizer
Execution time: 338458.499 ms
```