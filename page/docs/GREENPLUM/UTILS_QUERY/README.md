## Куратор раздела

<img align="left" width="200" src="../../png/shust.jpg" />

**Шустиков Владимир**, оставивший военную жизнь позади и ушедший в данные с головой. Работаю с данными более 2х лет и останавливаться не собираюсь! Веду:

   [Telegram канал](https://t.me/Shust_DE)
   
   [Youtube канал](https://www.youtube.com/@shust_de)

Если хочешь сменить текущую профессию на Дата Инженера — пиши не стесняйся, я сам проходил этот не легкий путь и тебе помогу https://t.me/ShustDE

Хочешь улучшить текущий раздел, внести недостающее или поправить формулировку? Предлагай PR и тегай [@ShustGF](https://github.com/ShustGF).

## Информация о текущих запросах

### Получение всех активных процессов

Одна строка для каждого серверного процесса c информацией по текущей активности процесса, такой как состояние и текущий запрос.

```sql
select * from pg_stat_activity;
```
### Информацию о запросах, и используемой ими ресурсами(ram, spill, время выполнения).

```sql
with search_spill_files_by_seg as
(
    select
       tt.sess_id,
       tt.usename,
       sum(tt.size) as spill_size_by_seg,
       tt.segid,
       tt.query
    from gp_toolkit.gp_workfile_entries tt
    group by tt.query, tt.sess_id, tt.usename, tt.segid
)
, search_spill_files as (
    select x.sess_id,
           x.usename,
           x.query,
           pg_size_pretty(sum(x.spill_size_by_seg)) as sum_spill_size,
           sum(x.spill_size_by_seg) as sum_spill_size_bytes
    from search_spill_files_by_seg x
    group by x.query, x.sess_id, x.usename
)
, base as (
       select
              pid,
              sa.sess_id,
              sa.usename,
              sa.query,
              query_start,
              extract(EPOCH from (now() - query_start)) as duration_in_seconds,
              round(extract(EPOCH from (now() - query_start)) / 60.0) AS duration_in_minutes,
              application_name,
              count(*) over(partition by sa.usename) as num_of_queries,
              coalesce(ssf.sum_spill_size, '0 bytes') as sum_spill_size,
              coalesce(ssf.sum_spill_size_bytes, 0) as sum_spill_size_bytes
       from pg_stat_activity as sa
           left join search_spill_files as ssf
		       on sa.sess_id = ssf.sess_id
       where state = 'active'
           and sa.usename not in :white_list
           and query_start is not null
)
select *
from base
where num_of_queries >= :queries_limit
    or duration_in_seconds >= :time_limit
    or sum_spill_size_bytes >= :spill_limit
order by usename
```
### Блокировки pg_locks

Какие запросы блокируются и чем они блокируются.

```sql
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocked_activity.query AS blocked_query,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocking_activity.query AS blocking_query,
    blocking_activity.state AS blocking_state,
    blocked_activity.application_name AS blocked_application,
    blocking_activity.application_name AS blocking_application
FROM pg_catalog.pg_locks blocked_locks
  JOIN pg_catalog.pg_stat_activity blocked_activity 
      ON blocked_locks.pid = blocked_activity.pid
  JOIN pg_catalog.pg_locks blocking_locks 
      ON blocked_locks.relation = blocking_locks.relation
      AND blocked_locks.locktype = blocking_locks.locktype
      AND blocked_locks.pid != blocking_locks.pid
  JOIN pg_catalog.pg_stat_activity blocking_activity 
      ON blocking_locks.pid = blocking_activity.pid
WHERE NOT blocked_locks.granted;
```

## Ресурсные группы

### View gp_toolkit.gp_resgroup_config
Конфигурация ресурсных групп.

```sql
select * 
from gp_toolkit.gp_resgroup_config;
```

### Сопоставление ролей и ресурсных групп

```sql
SELECT rolname, rsgname 
FROM pg_roles, pg_resgroup
WHERE pg_roles.rolresgroup=pg_resgroup.oid;
```

### Показатели утилизации ресурсов по ресурсным группам, хостам/сегментам

Текущие показатели утилизации ресурсов по ресурсным группам, хостам/сегментам.

```sql
SELECT now(), * 
FROM gp_toolkit.gp_resgroup_status_per_host 
ORDER BY (rsgname, hostname) ASC;
```

## Информация о таблицах

### Объем таблиц
```sql
SELECT 
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM 
    pg_stat_user_tables
WHERE 
    schemaname = <schemaname> and relname = '<relname>'
ORDER BY 
    pg_total_relation_size(relid) DESC
```

### Объем таблиц по сумме объемов партиций

```sql
WITH partition_list AS (
  SELECT
    partitiontablename AS partition_name
  FROM
    pg_partitions
  WHERE
    tablename = '<tableName>'
)
SELECT
	pg_size_pretty(SUM(pg_total_relation_size('<schemaName>.' || partition_name))) AS total_size
FROM
	partition_list;
```

### Распределение по сегментам

```sql
WITH segment_distribution AS (
    SELECT 
        gp_segment_id,
        COUNT(*) AS num_rows
    FROM 
        <schemaname>.<tablename>
    GROUP BY 
        gp_segment_id
)
SELECT
    gp_segment_id,
    num_rows,
    ROUND((num_rows * 100.0 / SUM(num_rows) OVER()), 2) AS percent_distribution
FROM
    segment_distribution
ORDER BY
    gp_segment_id;
```

## Информация о кластере

### Проверка RAM на хостах

```sql
SELECT 
	hostname,
	sum(memory_available::int) + 
    sum(memory_used::int) as mem_total,
  sum(memory_quota_available::int) + 
    sum(memory_quota_used::int) + 
    sum(memory_shared_available::int) + 
    sum(memory_shared_used::int) as mem_shared_plus_quota_total
FROM gp_toolkit.gp_resgroup_status_per_host group by hostname order by hostname;
```

## Информация по функция

### Вывод текста кода функции

```sql
SELECT pg_catalog.pg_get_functiondef(p.oid)
FROM pg_catalog.pg_proc p
WHERE p.proname = '<имя функции>' 
    AND pg_catalog.pg_function_is_visible(p.oid)
```