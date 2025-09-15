# 📘 Банк задач для собеседований по SQL

В этом разделе собраны практические задачи,
которые часто встречаются на собеседованиях.

---

## Задача Конкуренты

Есть таблица `competitors_prices` со следующими данными:

- sku_id_competitor — идентификатор SKU конкурента  
- competitor_id — идентификатор конкурента  
- promo_price_competitor — промо-цена конкурента  
- regular_price_competitor — регулярная цена конкурента  
- parsing_date — дата парсинга  

Нужно получить витрину, где для каждой комбинации `(sku_id_competitor, competitor_id, parsing_date)` остаётся **только одно значение цены**:
- если есть `promo_price_competitor` → берём её,  
- если промо отсутствует (`NULL`) → берём `regular_price_competitor`.

---

### DDL и наполнение таблицы

```sql
CREATE TABLE IF NOT EXISTS public.competitors_prices (
    sku_id_competitor     INT,
    competitor_id         INT,
    promo_price_competitor DECIMAL,
    regular_price_competitor DECIMAL,
    parsing_date          DATE
);

INSERT INTO competitors_prices VALUES
(15, 200, 147.0, 174.0, DATE '2025-08-30'),
(16, 200, NULL, 222.0, DATE '2025-08-27'),
(15, 200, NULL , 174.0, DATE '2025-08-30');
```

<details>
<summary>Показать результат</summary>

<table>
  <tr>
    <th>sku_id_competitor</th>
    <th>competitor_id</th>
    <th>price</th>
    <th>parsing_date</th>
  </tr>
  <tr>
    <td>15</td>
    <td>200</td>
    <td>147</td>
    <td>2025-08-30</td>
  </tr>
  <tr>
    <td>16</td>
    <td>200</td>
    <td>222</td>
    <td>2025-08-27</td>
  </tr>
</table>

</details>

<details>
<summary>Показать решение</summary>

```sql
WITH ranked AS (
    SELECT
        sku_id_competitor,
        competitor_id,
        promo_price_competitor,
        regular_price_competitor,
        parsing_date,
        COALESCE(promo_price_competitor, regular_price_competitor) AS price,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id_competitor, competitor_id, parsing_date
            ORDER BY CASE WHEN promo_price_competitor IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM public.competitors_prices
)
SELECT
    sku_id_competitor,
    competitor_id,
    price,
    parsing_date
FROM ranked
WHERE rn = 1;
```
</details>

***

## Задача: Запрос с использованием SELECT

Есть таблица `employees` с полями:  
- id  
- name  
- salary  
- department_id  

Нужно написать SQL-запрос, который возвращает имена сотрудников и
их зарплаты для всех сотрудников с зарплатой выше средней по всей таблице.
<details>
<summary>Показать решение</summary>

```sql
SELECT 
    name, 
    salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);
```
</details>

---

## Задача: Оконные функции и подзапросы

В таблице `employees` напишите запрос, который возвращает:  
- имя сотрудника  
- зарплату  
- ранг по зарплате внутри своего `department_id` (нумерация по убыванию зарплаты).

<details>
<summary>Показать решение</summary>

```sql
SELECT 
    name,
    salary,
    department_id,
    RANK() OVER (
        PARTITION BY department_id 
        ORDER BY salary DESC
    ) AS salary_rank
FROM employees;
```  
</details>

---

## Задача Последняя загрузка по таблице за день

Есть таблица public.load_status со следующими данными:
	•	dt — дата/время загрузки
	•	table_name — имя таблицы/вьюхи
	•	status — статус загрузки

Нужно построить выборку/витрину,
где для каждой таблицы в рамках каждого календарного дня
остаётся только одна строка — с максимальной dt
(если в день было несколько загрузок). Отфильтровать последние 14 дней.

```sql
DROP TABLE IF EXISTS public.load_status;

CREATE TABLE public.load_status (
    id          bigserial PRIMARY KEY,          -- уникальный ключ
    dt          timestamp with time zone NOT NULL,  -- время загрузки
    table_name  text NOT NULL,                  -- имя таблицы/вьюхи
    status      text NOT NULL                   -- статус загрузки
);

INSERT INTO public.load_status (dt, table_name, status) VALUES
-- Таблица alpha — два прогона в один день
('2025-09-15 09:15:00+03', 'alpha.orders_daily', 'OK'),
('2025-09-15 11:45:00+03', 'alpha.orders_daily', 'OK'),

-- Таблица beta — три прогона в один день
('2025-09-15 07:20:00+03', 'beta.customer_snapshot', 'OK'),
('2025-09-15 09:55:00+03', 'beta.customer_snapshot', 'OK'),
('2025-09-15 14:10:00+03', 'beta.customer_snapshot', 'OK'),

-- Таблица gamma — один прогон сегодня, два вчера
('2025-09-15 08:30:00+03', 'gamma.product_catalog', 'OK'),
('2025-09-14 10:05:00+03', 'gamma.product_catalog', 'OK'),
('2025-09-14 16:25:00+03', 'gamma.product_catalog', 'OK'),

-- Таблица delta — разные даты
('2025-09-13 06:40:00+03', 'delta.pricing_rules', 'OK'),
('2025-09-14 07:55:00+03', 'delta.pricing_rules', 'OK'),
('2025-09-15 12:20:00+03', 'delta.pricing_rules', 'OK'),

-- Таблица epsilon — только один прогон
('2025-09-15 10:10:00+03', 'epsilon.stock_levels', 'OK');
```

<details>
<summary>Показать результат</summary>
<table>
  <tr>
    <th>dt</th>
    <th>table_name</th>
    <th>status</th>
    <th>day</th>
  </tr>
  <tr>
    <td>2025-09-15 14:10:00+03</td>
    <td>beta.customer_snapshot</td>
    <td>OK</td>
    <td>2025-09-15</td>
  </tr>
  <tr>
    <td>2025-09-15 12:20:00+03</td>
    <td>delta.pricing_rules</td>
    <td>OK</td>
    <td>2025-09-15</td>
  </tr>
  <tr>
    <td>2025-09-15 11:45:00+03</td>
    <td>alpha.orders_daily</td>
    <td>OK</td>
    <td>2025-09-15</td>
  </tr>
  <tr>
    <td>2025-09-15 10:10:00+03</td>
    <td>epsilon.stock_levels</td>
    <td>OK</td>
    <td>2025-09-15</td>
  </tr>
  <tr>
    <td>2025-09-15 08:30:00+03</td>
    <td>gamma.product_catalog</td>
    <td>OK</td>
    <td>2025-09-15</td>
  </tr>
  <tr>
    <td>2025-09-14 16:25:00+03</td>
    <td>gamma.product_catalog</td>
    <td>OK</td>
    <td>2025-09-14</td>
  </tr>
  <tr>
    <td>2025-09-14 07:55:00+03</td>
    <td>delta.pricing_rules</td>
    <td>OK</td>
    <td>2025-09-14</td>
  </tr>
  <tr>
    <td>2025-09-13 06:40:00+03</td>
    <td>delta.pricing_rules</td>
    <td>OK</td>
    <td>2025-09-13</td>
  </tr>
</table>
</details>

<details>
<summary>Показать решение</summary>

```sql
WITH ranked AS (
    SELECT
        ls.dt,
        ls.table_name,
        ls.status,
        ls.dt::date AS day,
        ROW_NUMBER() OVER (
            PARTITION BY ls.table_name, ls.dt::date
            ORDER BY ls.dt DESC
        ) AS rn
    FROM public.load_status AS ls
    WHERE ls.dt >= current_date - INTERVAL '14 days'
)
SELECT
    dt,
    table_name,
    status,
    day
FROM ranked
WHERE rn = 1
ORDER BY dt DESC;
```
</details>

---

## Задача: Создание функции

Создайте функцию `get_department_salary_summary(dept_id INT)`, которая принимает ID отдела и возвращает таблицу с:  
- общим количеством сотрудников  
- средней зарплатой в этом отделе.

<details>
<summary>Показать решение</summary>

```sql
CREATE OR REPLACE FUNCTION get_department_salary_summary(dept_id INT)
RETURNS TABLE (
    employees_count INT,
    avg_salary NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) AS employees_count,
        AVG(salary) AS avg_salary
    FROM employees
    WHERE department_id = dept_id;
END;
$$ LANGUAGE plpgsql;
```  
</details>


