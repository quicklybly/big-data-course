# ДЗ 3. SQL поверх больших данных и инструменты визуализации БД

## Формулировка задания

**Цель**: освоить принципы оптимизации хранения и обработки аналитических данных в экосистемах Big Data

**Задание**: взять датасет от прошлого задания или свой, не меньше 1 ГБ. Сохранить в двух вариантах: CSV и Parquet с
партиционированием по дате.
Выполнить три аналитических запроса поверх обоих вариантов. Зафиксировать время выполнения и объём прочитанных данных.
Добавить фильтр по партиции и оставить только нужные колонки. Объяснить разницу через partition pruning и pushdown,
приложив план запроса.

**Форма отчётности**: собрать дашборд из трёх-четырёх виджетов в любом BI-инструменте. В отчёте объяснить, за счёт чего
запросы ускорились.

**Критерии оценивания** (max 6 б.): датасет ≥1 ГБ успешно сохранен в форматах CSV и Parquet с
партиционированием по дате, выполнены три аналитических запроса с фиксацией времени и объема прочитанных данных для
обоих вариантов, применены фильтр по партиции и проекция колонок с приложением планов запроса и объяснением механизмов
partition pruning и pushdown, создан дашборд из 3-4 виджетов в BI-инструменте, отчёт содержит корректное объяснение
причин ускорения запросов за счет оптимизаций хранения.

# Решение

## Инфраструктура

Стек технологий как на лекции - hive + trino + minio + superset.

Развернуто в docker'е:

```shell
docker compose up -d
docker ps
```

```text
CONTAINER ID   IMAGE                                                   STATUS                   PORTS                                                             NAMES
04f1b312ef98   lr3-superset:5.0.0                                      Up 7 minutes (healthy)   0.0.0.0:8088->8088/tcp, [::]:8088->8088/tcp                       lr3-superset
65b7751c7ebe   trinodb/trino:483                                       Up 7 minutes (healthy)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp                       lr3-trino
88318349faa2   apache/hive:4.2.1                                       Up 7 minutes (healthy)   0.0.0.0:9083->9083/tcp, [::]:9083->9083/tcp                       lr3-hive-metastore
080f9813f120   postgres:16                                             Up 8 minutes (healthy)   5432/tcp                                                          lr3-superset-db
2f989e242e5d   ghcr.io/coollabsio/minio:RELEASE.2025-10-15T17-29-55Z   Up 8 minutes (healthy)   0.0.0.0:9000-9001->9000-9001/tcp, [::]:9000-9001->9000-9001/tcp   lr3-minio
```

## Загрузка данных

Для генерации данных выполнить скрипт:

```shell
python3 generate_data.py
```

Скрип генерирует csv размером ~1.1ГБ. Таблица широкая на 22 колонки, чтобы было видно преимущество колоночного хранения
данных. Партиционирование по дням, события генерируются на 3 дня ⇒ 3 партиции.

Загружаем в s3.

```shell
docker compose up --force-recreate minio-init
```

Загружаем данные в trino.

```shell
docker exec -it lr3-trino trino
CREATE SCHEMA hive.hogwarts WITH (location = 's3a://warehouse/hogwarts/');

CREATE TABLE hive.hogwarts.events_csv (
    incident_id         varchar,
    created_at          varchar,
    created_date        varchar,
    student_name        varchar,
    house               varchar,
    school_year         varchar,
    professor           varchar,
    points_delta        varchar,
    reason              varchar,
    location            varchar,
    spell_cast          varchar,
    spell_success       varchar,
    wand_wood           varchar,
    wand_core           varchar,
    creature_involved   varchar,
    damage_galleons     varchar,
    detention_hours     varchar,
    witnesses_count     varchar,
    peeves_involved     varchar,
    filch_anger_level   varchar,
    owl_sent_to_parents varchar,
    howler_received     varchar
) WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/raw/events_csv/',
    skip_header_line_count = 1
);

CREATE TABLE hive.hogwarts.events_parquet
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['created_date']
) AS
SELECT
    CAST(incident_id AS bigint)           AS incident_id,
    CAST(created_at AS timestamp(3))      AS created_at,
    student_name,
    house,
    CAST(school_year AS integer)          AS school_year,
    professor,
    CAST(points_delta AS integer)         AS points_delta,
    reason,
    location,
    spell_cast,
    CAST(spell_success AS boolean)        AS spell_success,
    wand_wood,
    wand_core,
    creature_involved,
    CAST(damage_galleons AS decimal(10,2)) AS damage_galleons,
    CAST(detention_hours AS integer)      AS detention_hours,
    CAST(witnesses_count AS integer)      AS witnesses_count,
    CAST(peeves_involved AS boolean)      AS peeves_involved,
    CAST(filch_anger_level AS integer)    AS filch_anger_level,
    CAST(owl_sent_to_parents AS boolean)  AS owl_sent_to_parents,
    CAST(howler_received AS boolean)      AS howler_received,
    CAST(created_date AS date)            AS created_date
FROM hive.hogwarts.events_csv;

SELECT count(*) FROM hive.hogwarts.events_csv; -- 5400000
SELECT * FROM hive.hogwarts."events_parquet$partitions"; -- 3 partitions
SELECT created_date, count(*) FROM hive.hogwarts.events_parquet GROUP BY 1; -- 1.8M each
```

## Запросы к данным
