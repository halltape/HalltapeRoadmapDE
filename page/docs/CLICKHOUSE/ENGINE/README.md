## Куратор раздела

<img align="left" width="200" src="./../../png/shust.jpg" />

**Шустиков Владимир**, оставивший военную жизнь позади и ушедший в данные с головой. Работаю с данными более 3х лет и останавливаться не собираюсь! Веду:

   [Telegram канал](https://t.me/Shust_DE)
   
   [Youtube канал](https://www.youtube.com/@shust_de)

Если хочешь сменить текущую профессию на Дата Инженера — пиши не стесняйся, я сам проходил этот не легкий путь и тебе помогу https://t.me/ShustDE.

Хочешь улучшить текущий раздел, внести недостающее или поправить формулировку? Предлагай PR и тегай [@ShustGF](https://github.com/ShustGF).

## Движки ClickHouse

Ну что пришло время разбираться в движках ClickHouse и конечно же в их особенностях.
s
Все движки я разбираю на примерах, этому, чтобы понять как они работают, тебе будет нужно развернуть docker-compose распределённого Clickhouse и просто последовательно повротять все команды.

Приступим.

### SummingMergeTree

**SummingMergeTree** - таблица с группировкой одинаковых записей по ключу сортировки и применением суммы к перечисленным полям 

<details>
    <summary>Пример который ты можешь потыкать сам</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS summing_mt;

CREATE TABLE summing_mt
(
    id UInt32,
    val UInt32,
    dt datetime,
    example UInt32  -- столбец, не входящий в ключ сортировки и параметры движка
)
ENGINE = SummingMergeTree(val) -- сумма будет считаться по полю val, так как оно указано в качестве параметра движка 
ORDER BY (id)
PARTITION BY toYYYYMM(dt); -- записи по этому ключу будут группироваться

INSERT INTO summing_mt
SELECT 
    number % 2, 
    (number + 1) * 10, 
    now() + number * 60 * 60 * 24,
    (number + 1) * 100 
from numbers(30);
        </code>
    </pre>
</details>

Запомни что данные сливаются только в рамках **партиции**. Посмотри на данные

```SQL
SELECT * FROM summing_mt
```

Теперь снова добавь данных и выведи данные предыдущим запросом. Обрати внимание на разницу с предыдущем шагом до добавления данных.

```SQL
INSERT INTO summing_mt
SELECT 
    number % 2, 
    (number + 1) * 10, 
    now() + number * 60 * 60 * 24,
    (number + 1) * 100 
from numbers(30);
```

Как мы видим создался новый блок данных, теперь эти блоки необходими слить друг с другом. В движке происходит автомотическое суммирование данных, при "ленивом слиянии", но мы это сделаем вручную, следующей командой:

```SQL
OPTIMIZE TABLE summing_mt FINAL; -- ручное слияние
```

А теперь еще раз иди и посмотри что у тебя с данными. Да они слились в единое и соотвественно посчиталась общая сумма. Но ты же не будешь так жедать каждый раз, запуская ручное слияние данных, темболее у тебя не будет на это прав в реальном проекте, для этого есть ключевое слово **FINAL**.

```SQL
SELECT * FROM summing_mt FINAL
```

### AggregatingMergeTree

**AggregatingMergeTree** - это таблица, которая группирует одинаковые записи по ключу сортировки и применяет агрегатные функции к полям

#### Комбинаторы агрегатных функций

Перед тем как мы перейдём к рассмотрению данного движка, я хочу с того, что у агрегаторов есть так называемые [**Комбинаторы агрегатных функций**](https://clickhouse.com/docs/ru/sql-reference/aggregate-functions/combinators).

Если быть в кратце то данные комбинаторы дают достаточно большой спектр творчества при работе с аргигационными функциями, помните в PostgreSQL можно делать так 

```sql
...
count(Distinct col1) filter(where col1 > 1)
...
```

так вот в ClickHouse это можно сделать вот так:

```sql
...
countDistinctIf(col1, col1 > 1) 
...
```

Как ты понимаешь **-Distinct** и **-If** являются комбинаторами, их в целом очень много, так что обязательно перейди по ссылочке и посмотри что там есть.

Вообще комбинаторов очень много, а в связке с движком **AggregatingMergeTree** - это превращется в лютейшую пушку бомбу.

#### Агрегаторные типы данных

Как мы говорили в основной стать по CH, существуют 2 дополнительных типа данных **SimpleAggregateFunction** и **AggregateFunction** их предназночение очень просто.

- **SimpleAggregateFunction** - предназначет для хранения простых агрегатов, которое хранит конечно состояние
- **AggregateFunction** - сложные агрегаты, которая хранит состояние всех добавленных значений

Теперь рассмотрим сам движок и как он работает с данными типами данных.

##### SimpleAggregateFunction

Создаем таблицу с 2мя простыми агригационными функциями **MAX** и **SUM**. (в общем и целом простыми можно назвать все агрегаты которые могут в себе хранить конечное значение блока и без коализий агригироваться с конечными значениями других блоков)

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS simple;

CREATE TABLE simple (
    id UInt64, 
    val_sum SimpleAggregateFunction(sum, UInt64), -- предусмотрен для хранения простых агрегатов(хранит конечное состояние)
    val_max SimpleAggregateFunction(max, UInt32)
) 
ENGINE=AggregatingMergeTree 
ORDER BY id;

INSERT INTO simple SELECT  1, number, number from numbers(10);
INSERT INTO simple SELECT  2, sum(number), max(number) from numbers(5);
INSERT INTO simple SELECT  1, number, number from numbers(8);

        </code>
    </pre>
</details>

Выведим результат запроса и увидем, что движок агрегирует данные в блоке данных при вставке. В итоге мы увидем 3 строки

```sql
    SELECT * FROM simple
```

Но нам же нужно для ID=1 получить 1 строку. Можно дождаться следующего слияния, а можно выполнитьследующий запрос, которое сделает **логическое** слияние и выдаст конечный результат:

```sql
    SELECT * FROM simple FINAL
```

На больших таблицах? где данные поступаю большими блоками и достаточно часто, намного эффективнее получить тот же результат следущим запросом:

```sql
    SELECT 
        id, 
        sum(val_sum),
        max(val_max) 
    FROM simple
    GROUP BY id
```

##### AggregateFunction

Здесь немного все посложнее так как нужно использовать специальные комбинаторы.

Комбинаторы агрегаторных типов данных:
* SimpleState — возвращает результат агрегирующей функции типа SimpleAggregateFunction.
* State — возвращает промежуточное состояние типа AggregateFunction, используется при вставке.
* Merge — берёт множество состояний, объединяет их и возвращает результат полной агрегации данных.
* MergeState — выполняет слияние промежуточных состояний агрегации, возвращает промежуточное состояние агрегации.

По сути данные комбинаторы хранят состояния данных и выполнить просто команду ```SELECT *``` к данным таблицам не получится, давайте создадим таккую таблицу и попробуем достать выборку.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS aggr_func_tbl;

CREATE TABLE aggr_func_tbl
(
    id UInt64,
    val_uniq AggregateFunction(uniq, UInt64),         -- Хранит в себе промежуточное состояние данных
    val_any AggregateFunction(anyIf, String, UInt8),
    val_quant AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE=AggregatingMergeTree 
ORDER BY id;

INSERT INTO aggr_func_tbl
SELECT 
    1, 
    uniqState(toUInt64(rnd)),                 -- кол-во уникальных значений
    anyIfState(toString(rnd),rnd%2=0),
    quantilesState(0.5, 0.9)(toUInt64(rnd)) 
FROM
    (SELECT arrayJoin(arrayMap(i -> i * 10, range(10))) as rnd);

        </code>
    </pre>
</details>

Вот мы создали все что нам нужно, теперь достаём выборку:

```sql
    SELECT * FROM aggr_func_tbl
```

Ой ошибка, ай-ай-ай, а я же говорил. Не парься, чтобы получить какой-то результат, можно выполнить следующую команду:

```sql
    SELECT * FROM aggr_func_tbl  FORMAT Vertical
```

А теперь посмотри на данные, их же прочитать не возможно, поэтому смысла от того что мы сейчас делали нет. Чтобы достать необходимые для нас данные необходимо воспользоваться коомбинатом **-Merge**, вот так:

```sql
       SELECT uniqMerge(val_uniq), 
              quantilesMerge(0.5, 0.9)(val_quant), 
              anyIfMerge(val_any) 
       FROM aggr_func_tbl
```

### ReplacingMergeTree

Данный движок убирает дубликаты строк с одинаковым ключом сотрировки в блоке и во время слияния блоков.

Опять создаем таблицу и наполняем ее данными.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS replacing_merge_tree;


CREATE TABLE replacing_merge_tree
(
    id UInt32,
    dt date,
    val String
)
ENGINE = ReplacingMergeTree
ORDER BY (id); -- ключ по которому удаляются дубликаты

INSERT INTO replacing_merge_tree
VALUES (1, '2025-01-01', 'Djo'), (2, '2025-01-01', 'JB'),(3, '2025-01-01', 'JD');
        </code>
    </pre>
</details>

Теперь посмотри, что у тебя в данных:

```sql
    SELECT * FROM replacing_merge_tree
```

Не удивительно у тебя там 3 строки. Давайте теперь еще добавим пару строк, а потом посмотрим на результат.

```sql
    INSERT INTO replacing_merge_tree
    VALUES (1, '2025-01-02', 'Djo'), (1, '2025-01-03', 'Djo');

    SELECT * FROM replacing_merge_tree;
```

И что мы видим добавилась 1 из данных записей, потому что дубликаты по ключу удаляются в одном блоке и при слиянии. Давайте запустим ручное слияние и у нас останется только 3 строки.

```sql
    OPTIMIZE TABLE replacing_merge_tree;
```

Ну вот и все. Но что если нам нужно всегда оставлять только полследнее знание, для этого в движок можно передать колоку, относительно которой Clickhouse будет оставлять последнюю версию строки. Ну что еще раз создадим новую таблицу:

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS replacing_merge_tree_with_version;


CREATE TABLE replacing_merge_tree_with_version
(
    id UInt32,
    dt date,
    val String
)
ENGINE = ReplacingMergeTree(dt) -- колонка по которой необходимо оставить последнее
                                -- значение. Может быть либо числовой, либо датой.
ORDER BY (id); -- ключ по которому удаляются дубликаты

INSERT INTO replacing_merge_tree_with_version -- Добавляем первый блок данных
VALUES (1, '2025-01-01', 'Djo'), (2, '2025-01-01', 'JB'),(3, '2025-01-01', 'JD');

INSERT INTO replacing_merge_tree_with_version -- Добавлем 2й блок данных
VALUES (1, '2025-01-02', 'Djo'), (1, '2025-01-03', 'Djo');

OPTIMIZE TABLE replacing_merge_tree_with_version FINAL; -- Выполняем ручное слияние

SELECT * FROM replacing_merge_tree_with_version; -- смотрим и видим, что у ID=1, дата стоит 3 января 2025
        </code>
    </pre>
</details>

Именно таким образом можно поддерживать акутальное состояние данных.

### CollapsingMergeTree

**CollapsingMergeTree** -- Удаляет дубликабы строк по ключу сортировки в зависимости от флага. 

Наглядным примером будет приложение с книгами, где необходимо запоминать на какой странице остановился пользователь. Давай как раз и создадим такую таблицу.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS Books;

CREATE TABLE Books
(
    ID UInt64,
    Page UInt8,
    Sign Int8 -- имеет только 2 значения
)
ENGINE = CollapsingMergeTree(Sign) -- задаем флаг
ORDER BY ID
        </code>
    </pre>
</details>

Пользователь открывает книгу 1 на странице 1:

```sql
    INSERT INTO Books values (1, 1, 1);
```

Далее перелистывает на страницу 2:

```
    INSERT INTO Books values (1, 1, -1),(1, 2, 1);
```

Выполним логическое слияние блоков и сразу выведем результат на экран:

```sql
    SELECT * FROM Books FINAL
```

Остаётся актуальная страница книги


### Log

Ни чем не примичательный движок, предназначен для тестов и хранения небольших данных. Каждая котолонка такого движка хранится в отдельном файле.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS el;

CREATE TABLE el
(
    id UInt32,
    dt date
)
ENGINE = Log;

INSERT INTO el
select 
number,
now()::date + number,
from numbers(10);

SELECT * FROM el;
        </code>
    </pre>
</details>

### File

File -- Позволяет данные записывать в формате файла

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS ef;

CREATE TABLE ef
(
    id UInt32,
    dt date
)
ENGINE = File(CSV);

INSERT INTO ef
SELECT 
    number,
    now()::date + number
FROM numbers(10);

SELECT * FROM ef;
        </code>
    </pre>
</details>

Данная таблица сохранится как CSV файл, где лежали все таблицы внутри самого Clickhouse (путь к каталогу **/var/lib/clickhouse/data/ef/**).

### Buffer

Если у вас большое кол-во небольших вставок данных, то для ускорения процеса, чтобы не вызывалось часто слияние, необходимо использовать движок **Buffer**. Он сохраняет вставки в оперативную память после чего данные, сливаются на диск в другую таблицу.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS eb;

DROP TABLE IF EXISTS ebt;

CREATE TABLE ebt
    (
    id UInt32,
    dt date
)
ENGINE = Log;

CREATE TABLE eb
(
    id UInt32,
    dt date
)
ENGINE = Buffer(db,      -- имя БД
                ebt,     -- имя таблицы для слива данных
                16,      -- параллелизм (рекомендация 16)
                30,      -- минимальное время слияния
                60,      -- минимальное время слияния
                5,       -- минимальное кол-во строк для слияния
                10,      -- максимальное кол-во строк для слияния
                10000,   -- минимальное кол-во байт для слияния
                10000    -- максимальное кол-во байт для слияния
                );

INSERT INTO eb
SELECT 
    number,
    now()::date + number
FROM numbers(1);
        </code>
    </pre>
</details>

Читать данные из буфферной таблицы не получится.

```sql
    SELECT * FROM eb; -- выдаст ошибку
```

Выборку можно получить только из таблицы куда сливаются данные.

```sql
    SELECT * FROM ebt;
```

Чтобы слить данные из ОП в таблицу в ручную, используйте следующий запрос:

```sql
    OPTIMIZE TABLE eb;
```

### Memory

В движке Memory данныех храняться только в оперативной памяти, поэтому при перезапуске CH данные будут утеряны. Данный движок часто используется при оптимизации запроса.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS em;

CREATE TABLE em
(
    id UInt32,
    dt date
)
ENGINE = Memory;

INSERT INTO em
SELECT 
    number,
    now()::date + number
FROM numbers(100);

 SELECT * FROM em;
        </code>
    </pre>
</details>

### Set

Движок сет предназначен для использвоании в правой части оператора IN. Не хранит дублирующие заначения. Предназначен для оптимизации запросов.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS es;

CREATE TABLE es
    (
        id UInt32
    )
    ENGINE = Set
SETTINGS persistent = 1; -- данные будут считываться из ОП.

INSERT INTO es SELECT number from numbers(30);

-- Создание 2й таблицы
DROP TABLE IF EXISTS est;

CREATE TABLE est
(
    id UInt32
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO est SELECT number from numbers(300);
        </code>
    </pre>
</details>

Читать данные из  Set-таблицы не получится. 

```sql
    SELECT * FROM es -- выдаст ошибку 
```
Применять можно только таким образом:

```sql
    SELECT *
    FROM est
    WHERE id in es
```

Так же такая таблица может состоять из нескольких колонок, чтобы правильно к ней обратитсья, необходимо использовать следующий шаблон:

```sql
    SELECT *
    FROM table_name
    WHERE (col1, col2, ..., colN) in table_set
```

### GenerateRandom

**GenerateRandom** -- предназначен для генерации данных в СН, с целью дальнейших тестов.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS eg;

CREATE TABLE eg
(
    id UInt32, 
    val String,
    dt date,
    a Float32,
    b UUID,
    c Bool,
    d IPv6,
    e IPv4,
    g Array(UInt32)
)
ENGINE = GenerateRandom;

select * 
from eg
limit 10
        </code>
    </pre>
</details>

Можно запихнуть любой тип данных.

### PostgreSQL

**PostgreSQL** -- позволяет из CH подключаться к PSQL и применять функции CH над данными из PSQL.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS postgresql_table;

CREATE TABLE postgresql_table
(
    dept_id Int32,
    dept_name String,
    location String
)
ENGINE = PostgreSQL(
                    'postgres_server:5432', -- сервер, порт
                    'postgres',                           -- БД 
                    'table_name',                         -- таблица
                    'postgres',                           -- логин
                    'postgres',                           -- пароль
                    'public'                              -- имя схемы
                    );

select * from postgresql_table;
        </code>
    </pre>
</details>

Если запросы простые и PSQL может с ними справиться самомстоятельно то в CH транслируется только результат, если используются специфичные для CH функции, то PSQL транслирует все данные.

### Kafka

С помощью движка **Kafka** можно как отправлять данные в кафку, так и получать их из неё.

#### Отправка данных

Сейчас мы будем транслировать данные в кафку, дляначала создадим таблицу с нужным движком:

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS kafka_out_message;

 CREATE TABLE kafka_out_message
(
    id UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29093',     -- подключение к брокеру
    kafka_topic_list = 'clickhouse.topic', -- создай такой же топик
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONEachRow';
        </code>
    </pre>
</details>

Чтобы транслировать данные в кафку необходимо выполнить обычную команду на вставку:

```sql
INSERT INTO kafka_out_message
SELECT number FROM numbers(30);
```

ВСЁ!

#### Получение данных

Тут капец как муторно, но так придумано разработчиками, поэтому мы просто делаем свою работу и не более того. Создадим очередную таблицу:

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS kafka_input_stage;

CREATE TABLE kafka_input_stage
    (
        json_kafka String    -- задается только 1 строка с полным JSON-ом из кафки
    )
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29093',
    kafka_topic_list = 'source.public.order_events',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONAsString'; -- достаточно часто истользуется именно этот формат
        </code>
    </pre>
</details>

Считать данные из этой таблицы просто так не получится, данная таблица сохраняет данные в памяти и может только перенаправлять данные в нужную нам таблицу с определённым методом хранения(движком).

Если всетаки хочется пороверить, а получаем ли мы что то с данного топика, необходимо выполнить следующий запрос:

```sql
SET stream_like_engine_allow_direct_select = 1
SELECT * FROM kafka_input_stage;
```

Теперь создаем таблицу в которой будут непосредственно хранитсья данные:

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP TABLE IF EXISTS table_stage_from_kafka;

CREATE TABLE table_stage_from_kafka 
(
    json_kafka String
)
Engine = Log
        </code>
    </pre>
</details>

Так же нужно создать **материализованное представление**, которое будет перенаправлять данные из таблицы с движном **Kafka** в таблицу с движком **Log**.

<details>
    <summary>Выполни данный скрипт</summary>
    <pre>
        <code class="sql">
DROP VIEW IF EXISTS kafka_input_stage_mv;

CREATE MATERIALIZED VIEW kafka_input_stage_mv TO table_stage_from_kafka AS 
    SELECT
        json_kafka
    FROM kafka_input_stage;
        </code>
    </pre>
</details>

Осталось только достать данные из JSON:

```sql
SELECT  
    JSONExtractInt(JSONExtractString(json_kafka ,'before'), 'id') as before_id,  -- JSONExtract<тип данных>(<JSON строка>, <ключ>)
    JSONExtractInt(JSONExtractString(json_kafka ,'before'), 'order_id') as before_order_id,
    JSONExtractString(JSONExtractString(json_kafka ,'before'), 'status') as before_status,
    JSONExtractUInt(JSONExtractString(json_kafka ,'before'), 'ts') as before_ts,
    JSONExtractInt(JSONExtractString(json_kafka ,'after'), 'id') as after_id,
    JSONExtractInt(JSONExtractString(json_kafka ,'after'), 'order_id') as after_order_id,
    JSONExtractString(JSONExtractString(json_kafka ,'after'), 'status') as after_status,
    JSONExtractUInt(JSONExtractString(json_kafka ,'after'), 'ts') as after_ts,
    JSONExtractString(json_kafka ,'op') as op,
    toDateTime(JSONExtractInt(json_kafka ,'ts_ms') / 1000) as dt  -- Перевод времени из timestamp в читаемый вид
FROM table_stage_from_kafka
```

### ReplicatedMergeTree

Данный движок позволяет положить копию таблицы на репликационный сервер. Данный сервер предназначен для повышения отказоустойчивости всего кластера. Важно помнить, что репликация не происходит моментально, согласованность основного и репликационного сервера происходит со временем и может произойти ситуация, когда на основном сервере произошел сбой, далее перешли на реплику, а данные на енй не в полном объеме. Это абсолютно нормально для ClickHouse.

Создается такая таблица с определёнными особенностями, при создании объекста необходимо использваоть конструкцию **ON CLUSTER 'имя кластера'**. Это конструкция копирует объект на каждый сервер всего кластера.

```sql
CREATE TABLE events ON CLUSTER 'company_cluster' (   -- имя кластера можно задавать по имени и с помощью макроса '{cluster}'
        time DateTime,
        uid  Int64,
        type LowCardinality(String)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/events', '{replica}') -- путь может быть любой, необходимо смотреть как положено это делать в вашей команде.
    PARTITION BY toDate(time)
    ORDER BY (uid);
```

### Distributed

В продолжении рапределённых таблиц мы не можем не остановиться на теме шардирвания кластера. Шардирование -- это по свой сути обыкновенное разделение даных по серверам, по определенному признаку, для повышения вычислительных способностей всего кластре, т.е. мы ускоряем работу выполенения запросов(но не всех! JOIN и оконные функции будут **ебать мозги** если распределить данные не по ключу соединения и не по ключую разбития окна(PARTITION BY), соотвествено). 

Сама **Distributed** таблица не хранит в себе ничего, она исключительно ссылается на таблицу которая хранит в себе данные. Для примера как хранимую таблицу, мы возьмем из предыдущего запроса "events". Данная таблица будет хранить только те данные которые ей даст **Distributed** таблица на определённой шарде.

Созадим Distributed таблицу:

```sql
CREATE TABLE events_distr ON CLUSTER 'company_cluster' AS events
ENGINE = Distributed('company_cluster', -- имя кластера, можно исполльзовать макрос
                     db,                -- имя БД
                     events,            -- имя таблицы
                     uid);              -- имя колонки по которой будет распределять данные
```

Конструкция **AS events**, говорит создай таблицу **events_distr**, с такимиже полями как и таблица events. Очень важно, чтобы колонки совпадали.

```sql
    INSERT INTO events_distr VALUES
        ('2020-01-01 10:00:00', 100, 'view'),
        ('2020-01-01 10:05:00', 101, 'view'),
        ('2020-01-01 11:00:00', 100, 'contact'),
        ('2020-01-01 12:10:00', 101, 'view'),
        ('2020-01-02 08:10:00', 100, 'view'),
        ('2020-01-03 13:00:00', 103, 'view');
```

Теперь считай данные и распределённой таблицы(events_distr) и из локальной таблицы(events). Ты увидешь что в локальной не хватает данных из-за того что другие данные хранять на другой шарде.