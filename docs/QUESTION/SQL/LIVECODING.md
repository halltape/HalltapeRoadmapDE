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


