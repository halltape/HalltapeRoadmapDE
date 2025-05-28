# Clickhouse Cluster

Кластер Clickhouse был честно спизжен с данного ресурса, ибо нахер придумывать велосипед когда и так всё хорошо работает

## Запуск

Установите пакет **make** и перейдите в каталог с кластером.

```sh
sudo apt install make
```

Для поднятия всего кластера необходимо выполнить одну единственную команду:

```sh
make config up
```

В итоге поднимится кластер с именем `company_cluster`.

Контейнер находится в сети `172.23.0.0/24` (эта инфа нужна если вы делаете пет-проект и вы разграничиваете сети)

| Container    | Address
| ------------ | -------
| zookeeper    | 172.23.0.10
| clickhouse01 | 172.23.0.11
| clickhouse02 | 172.23.0.12
| clickhouse03 | 172.23.0.13
| clickhouse04 | 172.23.0.14

## Профиля

- `default` - no password
- `admin` - password `123`

## Пример подключения через DBeaver

<p align="center">
    <img src="./../../../png/ch_con_settings.jpg" alt="Пример подключения" width="600"/>
</p>

## Старт и останвка

Старт и остановка контейнерос
```sh
make start
make stop
```

## Полная остановка

Полная остановка с удалением контейнера
```sh
make down
```
## Дополнительная конфигурация

Полазте по проекту, там нет абсолютно ничего сложного. Если захотите настройте его под себя. 

А с другоой стороны:

[![Не лезь бл*ть оно тебя сожрёт](https://markdown-videos-api.jorgenkh.no/youtube/rTSODCT4mKw)](https://www.youtube.com/watch?v=oqRuyaVv_0A&list=PLYtH_gZiwIjiKTGP-4gI75NL4fTXZHker&rco=1)