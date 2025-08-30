<!-- Yandex.Metrika counter -->
<script type="text/javascript">
    (function(m,e,t,r,i,k,a){
        m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
        m[i].l=1*new Date();
        for (var j = 0; j < document.scripts.length; j++) {if (document.scripts[j].src === r) { return; }}
        k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)
    })(window, document,'script','https://mc.yandex.ru/metrika/tag.js?id=103580753', 'ym');

    ym(103580753, 'init', {ssr:true, webvisor:true, clickmap:true, ecommerce:"dataLayer", accurateTrackBounce:true, trackLinks:true});
</script>
<noscript><div><img src="https://mc.yandex.ru/watch/103580753" style="position:absolute; left:-9999px;" alt="" /></div></noscript>
<!-- /Yandex.Metrika counter -->
## Куратор раздела

<img align="left" width="200" src="../png/shust.jpg" />

**Шустиков Владимир**, оставивший военную жизнь позади и ушедший в данные с головой. Работаю с данными более 2х лет и останавливаться не собираюсь! Веду:

   [Telegram канал](https://t.me/Shust_DE)
   
   [Youtube канал](https://www.youtube.com/@shust_de)

Если хочешь сменить текущую профессию на Дата Инженера — пиши не стесняйся, я сам проходил этот не легкий путь и тебе помогу https://t.me/ShustDE.

Хочешь улучшить текущий раздел, внести недостающее или поправить формулировку? Предлагай PR и тегай [@ShustGF](https://github.com/ShustGF).

## Напутственные слова перед изучением материала

**!!!Сюда стоит лезть, после изучения курсов и статей по Python, SQL, Docker, Командной строки Linux!!!**

Данная статья охватывает основы работы с оркестратором **AirFlow**. Рассмотрим кратко теорию, которую спрашивают достаточно часто, а так же посмотрим, как локально развернуть AirFlow с помощью **docker-compose** в простом примере одного из тестовых заданий по AirFlow.

Как говорил мой дед: "Я твой дед!". Вы можете спросить, "А к чему ты это написал?", а я вам отвечу "Хз. Живите теперь с этим!"

Если после прочтения статьи, вы захотите погрузиться в AirFlow глубже, то могу порекомендовать книгу ["Apache Airflow и конвейеры обработки данных"](../files/Apache%20Airflow.pdf). В ней описывается работа с AirFlow достаточно подробно на примерах.

Приятного погружения. 

-----------------
<h1 style="text-align: center;">AIRFLOW</h1>

Возьмём определение с [официального репозитория AirFlow](https://github.com/apache/airflow).

**AirFlow** — это платформа для программирования, планирования и мониторинга рабочих процессов.

А в общем и целом нужно запомнить, что AirFlow это **оркестратор** (не ELT-инструмент), в котором есть возможность прописывать ETL процессы, на языке Python. Каждый такой процесс представляет собой DAG, состоящий из определённых задач.

<p align="center">
    <img src="./../png/airflow_logo.jpg" alt="AirFlow" />
</p>

## DAG

DAG(Directed Acyclic Graph, направленный ациклический граф) представлет из себя набор **тАсок**(задач), идущих последовательно друг за дружкой, либо параллельно, которые нельзя зациклить по кругу, т.е. своего рода строится прямолинейный конвеер обработки данных.

<p align="center">
    <img src="./../png/af_dag.png" alt="AirFlow" />
</p>

## Архитектура AirFlow

AirFlow состоит из четырёх основных, взаимосвязанных компонентов:

* Планировщик AirFlow(Scheduler)
* Исполнитель(Executor)
* Воркеры (Workers)
* Веб-сервер AirFlow

<p align="center">
    <img src="./../png/af_arfitecture.png" alt="AirFlow" />
</p>

### Задачи планировщика

- Анализ графа;
- Проверка параметра **scheduler_interval**, который определяет частоту выполнения DAG;
- Планирование очереди графов.

### Задачи Исполнителя

- Запуск задач; 
- Распеделение задач между воркерами.

### Задачи воркеров

- Получает задачи от исполнителя;
- Отвечает за полное выполнение задач.

### Задачи Веб-сервера AirFlow

- Визуализация DAG'а, который проанализировал планировщик;
- Предоставлять интерфейс пользователю для отслеживания работы DAG.

## Инициализация DAG и его основные параметры

В Python-файле DAG инициализируется очень просто, необходимо импортировать класс DAG из библиотки AirFlow:

```python
from airflow import DAG
```

И далее создать экземпляр объекта. Обычно переменную экземпляра принято называть **dag**.

```python
dag = DAG(
    перечисление свойств
)
```

Именно этот экземпляр класса передаёются в дальнейшем каждому оператору, так оператор понимает, что принадлежит именно этому экземпляру.

```python
PythonOperator(
  ...
  dag=dag
)
```

### Свойства экземпляра DAG

У экземпляра DAG есть большое количество свойств, но остановлюсь я только на 3х, постоянно встречающихся в каждом DAG-экземпляре

1. **dag_id** — идентификатор DAG. Именно этот идентификатор проставляется в имени DAG на главной странице Airflow.

```python
dag = DAG(
    dag_id = "load_file_to_psql"
    ...
)
```

2. **start_date** — задается дата и время начала планирования запусков DAG. Обычно для задания даты используется библиотека **datetime**.

```python
import datetime as dt

dag = DAG(
    dag_id = "load_file_to_psql"
    start_date = dt.datetime(2024, 11, 13)
    ...
)
```

3. **schedule_interval** — планируемое время запуска DAG. 

```
import datetime as dt

dag = DAG(
    dag_id = "load_file_to_psql"
    start_date = dt.datetime(2024, 11, 13)
    schedule_interval = dt.timedelta(days=5) # раз в 5 дней, т.е. 13, 18, 23 и т.д.
    ...
)
```

В **schedule_interval** есть несколько способов задать время запуска. Ради примера в скобках будет указан ежедневный интервал.

* **CRON** — о нём во всех подробностях описано [здесь](../Linux/README.md). (schedule_interval = '0 0 * * *')
* **timedelta** — из библиотеки **datetime** (schedule_interval = dt.timedelta(days=5))
* макросы (schedule_interval = '@daily')
  
|**Макрос**|**Значение**|
|---|---|
|@once| Один и только один раз |
|@hourly| Запуск один раз в час в начале часа |
|@daily| Запуск один раз в день в полночь |
|@weekly| Запуск один раз в неделю в полночь в воскресенье утром|
|@monthly| Запуск один раз в месяц в полночь первого числа месяца|
|@yearly| Запуск один раз в год в полночь 1 января|

Если мы хотим запускать DAG вручную, то необходимо прописать значение **None** (schedule_interval = None)

## Операторы AirFlow

Как вам уже известно, конвеер обработки является направленным ацикличным графом, в свою очередь DAG состоит из определённых задач, которые определяются **операторами**. 

В AirFlow существует множество операторов, с их полным списком можно ознакомиться [здесь](https://airflow.apache.org/docs/apache-airflow-providers/operators-and-hooks-ref/index.html)

Вот примеры часто встречаемых операторов, с которыми мы познакомимся в примере ниже:

* PythonOperator
* BashOperator
* PostgresOperator

Думаю, из названия и так понятно, для чего они нужны. Так что тут без комментариев

## Передача данных между задачами.
 
Существует 2 метода передачи данных между тасками в AirFlow:

1. Механизм XCom;
2. Сохранение данных в хранилищах.

### Механизм XCom

XCom - позволяет обмениваться сообщениями между задачами. Предназначен он исключительно для **небольших данных**. 

Согласно документации в зависимости от используемой базы данных метаинформации:

* SQLite - до 2х Гб.
* PostgreSQL - до 1 Гб.
* MySQL - до 64 Кб.

**!!!Запомните!!!**
XCom можно использовать для передачи небольших объемов данных, например, значение агрегации, количества строк в файле, даже можно небольшой файл передать, но в остальных случаях используйте внешние решения для хранения данных, как пример, сохраняйте все в каталог **tmp** и потом забирайте данные оттуда.

Определяется Xcom 2мя способами:

* с помощью команд **xcom_push** и **xcom_pull**(с этим методом мы познакомимся в примере)
* с помощью **Taskflow API** (декоратор **@task**, вот хорошая [статься](https://bigdataschool.ru/blog/taskflow-api-in-apache-airflow.html) с примером)

## Пример тестового задания

Необходимо написать DAG, который будет выполнять следующие задачи:

``` 
1. С помощью PythonOperator необходимо сгенерировать тестовые данные и записать их в файл в каталог /tmp/data.csv ( для простоты можно взять 2 колонки - id, value )
2. С помощью BashOperator переместить файл в каталог /tmp/processed_data
3. C помощью PythonOperator нужно загрузить данные из файла в таблицу в Postgres ( таблицу можно предварительно создать )
4. После записи данных в таблицу последним таском выведите в логах сообщение о количестве загруженных данных.
 
С помощью XCom необходимо:
 
Передать путь до файла из п.1 в оператор в п.2.
Передать количество записей из п.3 в п.4
```

### Запуск DAG'а

Для начала развернём с помощью Docker Compose, **AirFlow**. 

Жми и копируй [docker-compose.yaml](./task/docker-compose.yaml) в свой каталог.

С помощью WSL или любой командной строки, где ты работаешь с Docker, переходишь в каталог со скаченным или скопированным файлом и выполняешь команду:

```
docker compose up -d
```

Пока у тебя качаются и поднимаются контейнеры, скачай следующий файл [test_task.py](./task/test_task.py)

После того, как контейнеры поднимутся, в каталоге с **docker-compose.yaml** появятся 4 каталога config, dags, logs, plugins.

Необходимо скаченный файл **test_task.py** переместить в каталог **dags**.

Теперь заходим в браузер и в URL-строке прописываем:

```
localhost:8080
```

Тем самым переходим на стартовую страницу.

<p align="center">
    <img src="./../png/af_start_page.png" alt="AirFlow" />
</p>


```
Логин: airflow
Пароль: airflow
```

После входа нас перебрасывает на главную страницу, где мы увидим DAG, который мы скинули в каталог(если его там нет, подождите минут 5 и обновите страницу, в крайнем случае рестартаните Docker Compose)

Перед тем, как запустить DAG, необходимо прописать коннект к БД PostgreSQL, которая присутствует в Docker Compose файле. 

Для это в верхней строке меню наводим курсор на **Admin** и выбираетм пункт **Connections**. Нажимаем синий плюсик в открывшимся окне подключения, прописываем следующие параметры:

|Параметр| Значение|
|---|---|
|Connection Id|psql_connection|
|Connection Type|Postgres|
|Host|host.docker.internal|
|Database|user|
|Login|user|
|Password|user|
|Port|5431|
|Extra|могут быть кавычки, необходимо оставить пустое поле, иначе будет ошибка подключения.|


<p align="center">
    <img src="./../png/af_connection.png" alt="AirFlow" />
</p>

Нажимаем Save

Возвращаемся на главную страницу и заходим в наш DAG, жамкув по его имени. 

Далее запускаем DAG, нажав на **Trigger DAG**(треугольник) в правом верхнем углу страницы.

Всё поздавляю, вы запустили свой первый DAG, теперь разберёмся с кодом, написанным в DAG'е.

### Поясняю за код

Для начала необходимо инициализировать DAG с помощью класса DAG, для этого импортируем класс из библиотеки airflow.

```
from airflow import DAG
```
Инициализация DAG происходит следующим образом:

```
dag = DAG(
    dag_id="load_file_to_psql",
    default_args=default_args
)
```
Чтобы не захламлять сам класс DAG, аргументы можно передать через параметр **default_args** и объявить словарь с параметрами отдельно.

```
default_args = {
  'owner': '@Shust_DE',
  'depends_on_past': False,
  'start_date': datetime.now(),
  'email': ['https://t.me/Shust_DE'],
  'schedule_interval': "@hourly",
}
```

Далее берём первый пунт задания. Для генерации файла я использую библиотеку **pandas**. Следующий код генерирует данные и сохраняет их в файл.

```
import pandas as pd

path_filename = '/tmp/data.csv'
table = [(i, md5(int(i).to_bytes(8, 'big', signed=True)).hexdigest()) for i in range(1, 100)]
table = pd.DataFrame(table, columns=['id', 'md5_id'])
table.to_csv(path_filename, index=False)
```

Помещаем данный фрагмент в функцию **_generate_file**. Но не забываем, что путь к файлу нам необходимо передать через XCom. Поэтому необходимо в функцию передать все аргументы DAG'а, т.е. переменную **kwargs**. Из данного словаря нам нужен ключ **ti** (или task_instans, это одно и тоже, ссылка на один и тот же объект). Поэтому приравниваем его к переменной **ti**, чтобы не запутаться.

```
ti = kwargs['ti']
```

Далее необходимо у объекта вызвать метод **xcom_push** и передать ключ и значение XCom переменной.

```
ti.xcom_push(key='path_file', value=path_filename)
```

Объединяем и получаем полноценную функцию **_generate_file**:

```
def _generate_file(**kwargs):
  ti = kwargs['ti']
  path_filename = '/tmp/data.csv'
  table = [(i, md5(int(i).to_bytes(8, 'big', signed=True)).hexdigest()) for i in range(1, 100)]
  table = pd.DataFrame(table, columns=['id', 'md5_id'])
  table.to_csv(path_filename, index=False)
  ti.xcom_push(key='path_file', value=path_filename)
```
В PythonOperator нам просто нужно передать данную функцию следующим образом:

```
generate_file = PythonOperator(
    task_id='generate_file',
    python_callable=_generate_file,
    dag=dag,
)
```

За второй пункт отвечает следующая конструкция:

```
move_data_file = BashOperator(
  task_id="move_data_file",
  bash_command=("mkdir -p /tmp/processed_data/ && "
          "mv {{ ti.xcom_pull(task_ids='generate_file', key='path_file') }} /tmp/processed_data/"),
  dag=dag,
)
```

Здесь применяется команда создания каталога и проверки его на существование

```
mkdir -p /tmp/processed_data/
```

И, соотвественно, перемещение самого файла

```
mv {{ ti.xcom_pull(task_ids='generate_file', key='path_file') }} /tmp/processed_data/
```

Как можно заметить, так как нам нужно достать путь к файлу из XCom в Bash-конструкцию, мы воспользовались Jinja-шаблонизатором для языка Python.

Для пункта 3 изначально создается таблица с помощью оператора PostgresOperator в конструкции:

```
create_table_psql = PostgresOperator(
	task_id='create_table',
    postgres_conn_id='psql_connection',
    sql=""" DROP TABLE IF EXISTS table_name;
            CREATE TABLE table_name ( id int,
                                      md5_id text);
        """
)
```

Думаю, тут и так все понятно и комментарии излишни. А если ты не знаешь, что делает SQL код, то какого лешего ты тут забыл, иди учи SQL!

Для загрузки данных используется следующий код:

```
df = pd.read_csv("/tmp/processed_data/data.csv")
engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
df.to_sql('table_name', engine, if_exists='append', schema='public', index=False)
```

Но так как у нас есть дополнительное условие, а именно в XCom переменную положить количество строк в файл, то опять обращаемся к **ti**.

```
ti = kwargs['ti']
ti.xcom_push(key='count_string', value=len(df))
```

Объединяем, и получается следующая функция **_data_in_postgres**:

```
def _data_in_postgres(**kwargs):
  ti = kwargs['ti']
  df = pd.read_csv("/tmp/processed_data/data.csv")
  engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
  df.to_sql('table_name', engine, if_exists='append', schema='public', index=False)
  ti.xcom_push(key='count_string', value=len(df))
```

Теперь необходимо вывести просто количество строк, делаем все тоже самое, как и при переносе файла. За это отвечается конструкция:

```
print_count_string_in_df = BashOperator(
  task_id="print_count_string_in_df",
  bash_command=('''echo "В таблице {{ ti.xcom_pull(task_ids='data_in_postgres', key='count_string') }} строк" '''),
  dag=dag,
)
```

Ну и наконец, нужно построить граф, для этого последовательно выстраиваем операторы:

```
(
    generate_file >> 
    move_data_file >> 
    create_table_psql >> 
    data_in_postgres >> 
    print_count_string_in_df
)
```

Чтобы посмотреть результат последнего пункта, во вкладке **graph** выдели последнюю зелёную таску (print_count_string_in_df), и появятся дополнительные вкладки. Перейдите во вкладку **Logs**, и вы увидите запись, представленную на скриншоте.

<p align="center">
    <img src="./../png/af_final_task.png" alt="AirFlow" />
</p>
