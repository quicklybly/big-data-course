# ДЗ 5. Продвинутый Spark

Домашнее задание направлено на практическое освоение техник оптимизации производительности Spark-приложений и
формирование понимания того, как осознанно управлять ресурсами кластера без сложной настройки инфраструктуры. Вы
возьмёте уже работающий Spark-job из предыдущего задания и проведёте серию экспериментов по его ускорению, применяя
ключевые приёмы оптимизации, которые используются в production-среде.

**Цель**: понять, как влиять на производительность Spark‑приложений, без сложной настройки кластера.
Задание:взять уже работающий Spark‑job (из ДЗ №4) и применить:

- cache()/persist()  к промежуточному RDD/DF,
- изменить количество партиций (.repartition()  или .coalesce() ).
- Зафиксировать время до/после (приблизительно, если локально и на небольших данных).

**Форма отчетности**: результатом должен быть отчет, который содержит сравнение времени выполнения уже работающей
Spark‑job'ы (из ДЗ №4) до и после применения cache()/persist()  к промежуточному RDD/DF и до и после изменения
количества партиций с помощью .repartition()  или .coalesce()

**Критерии (max 6 б.)**: корректно применены cache()/persist() к промежуточным данным, изменено количество партиций с
помощью. repartition() или .coalesce(), представлены замеры времени выполнения задачи до и после оптимизации, дано
пояснение влияния применённых изменений на производительность, код успешно выполняется.

## Инфраструктура

Spark 4.3.0 в локальном режиме (`local[*]`, 18 ядер), данные - лог из ДЗ 4 (`lab4/data/access.log`, 1kk строк, 140 MB).
Из двух реализаций ДЗ 4 оставлена только DataFrame-версия.

```shell
cd lab4/spark && ./gradlew generateLog
cd lab5/spark && ./gradlew dfBench       # все режимы, 5 прогонов; -Pruns=N -Pmodes=baseline,cache-parsed,...
```

- Код: [DataFrameExperiment.java](spark/src/main/java/com/quicklybly/bigdata/spark/DataFrameExperiment.java)
- Бенчмарк: [Bench.java](spark/src/main/java/com/quicklybly/bigdata/spark/Bench.java)
- Результаты: `lab5/data/df-bench.txt`.

## Задача

Job из ДЗ 4 (число запросов по IP) расширен, чтобы промежуточный DataFrame
переиспользовался несколькими actions:

```java
Dataset<Row> parsed = spark.read().text(input)          // промежуточный DF
        .filter(col("value").rlike(LogFormat.IP_REGEX))
        .select(
                regexp_extract(col("value"), LogFormat.LINE_REGEX, 1).as("ip"),
                regexp_extract(col("value"), LogFormat.LINE_REGEX, 3).as("url"),
                regexp_extract(col("value"), LogFormat.LINE_REGEX, 4).as("status"),
                regexp_extract(col("value"), LogFormat.LINE_REGEX, 5).as("bytes"));
Dataset<Row> counts = parsed.groupBy("ip").count();

counts.count();
counts.agg(sum("count")).first();
counts.orderBy(desc("count")).limit(10);
parsed.groupBy("status").count();
parsed.groupBy("url").count().orderBy(desc("count"));
parsed.agg(sum("bytes")).first();
counts.orderBy("ip").coalesce(1).write().csv(output);
```

## Методика изменерения

- Один `SparkSession`, перед замерами один прогревочный прогон
- 5 раундов тестирования, режимы внутри раунда чередуются, берётся медианное значение
- AQE (`spark.sql.adaptive.enabled`) выключен везде, кроме режима `aqe`, - иначе Spark сам меняет число
  партиций после shuffle
- Каждый прогон проходит проверку на корректность

## Варианты

| режим                     | что изменено                                                                                             |
|---------------------------|----------------------------------------------------------------------------------------------------------|
| `baseline`                | baseline: без кеша, после shuffle 200 партиций (значение по умолчанию), AQE выключен                     |
| `cache-counts`            | Кешируем готовый результат по IP (`counts.cache()`): 4 action'а из 7 используют кеш                      |
| `cache-parsed`            | Кешируем парсинг лога (`parsed.persist(MEMORY_ONLY)`)                                                    |
| `coalesce-18`             | После каждой группировки склеиваем 200 партиций в 18 (`.coalesce(18)`), shuffle при этом не меняется     |
| `repartition-ip-18`       | Перед подсчётом по IP заранее раскладываем строки по IP на 18 партиций (`.repartition(18, col("ip"))`)   |
| `shuffle-18`              | Shuffle сразу делит данные на 18 партиций по числу ядер, а не на 200 (`spark.sql.shuffle.partitions=18`) |
| `cache-parsed+shuffle-18` | Кеш распарсенного лога + 18 shuffle-партиций                                                             |
| `cache-counts+shuffle-18` | Кеш результата по IP + 18 shuffle-партиций                                                               |
| `cache-both+shuffle-18`   | Кешируем и распарсенный лог, и результат по IP + 18 shuffle-партиций                                     |
| `aqe`                     | Как baseline, но AQE включён (по умолчанию в Spark 4): Spark сам объединяет мелкие партиции на ходу      |
| `coalesce-2`              | Антипример: работают только 2 ядра из 18                                                                 |

18 тк, выделено 18 ядер.

## Результаты

| режим                     | прогоны, s                       | медиана, s | vs baseline | партиций: вход / per-IP |
|---------------------------|----------------------------------|-----------:|------------:|-------------------------|
| `baseline`                | 2.65 / 2.08 / 2.11 / 2.04 / 2.10 |   **2.10** |           - | 18 / 200                |
| `cache-counts`            | 1.89 / 1.59 / 1.46 / 1.53 / 1.40 |   **1.53** |        −27% | 18 / 200                |
| `cache-parsed`            | 2.87 / 1.91 / 1.95 / 1.86 / 1.99 |   **1.95** |         −7% | 18 / 200                |
| `coalesce-18`             | 2.28 / 2.08 / 2.15 / 2.13 / 2.22 |   **2.15** |         +2% | 18 / 18                 |
| `repartition-ip-18`       | 2.42 / 2.01 / 1.82 / 1.92 / 1.92 |   **1.92** |         −9% | 18 / 18                 |
| `shuffle-18`              | 0.88 / 0.75 / 0.80 / 0.76 / 0.84 |   **0.80** |        −62% | 18 / 18                 |
| `cache-parsed+shuffle-18` | 0.72 / 0.67 / 0.68 / 0.63 / 0.76 |   **0.68** |        −68% | 18 / 18                 |
| `cache-counts+shuffle-18` | 0.53 / 0.51 / 0.56 / 0.52 / 0.52 |   **0.52** |    **−75%** | 18 / 18                 |
| `cache-both+shuffle-18`   | 0.55 / 0.50 / 0.54 / 0.50 / 0.51 |   **0.51** |    **−76%** | 18 / 18                 |
| `aqe`                     | 1.82 / 1.40 / 1.40 / 1.41 / 1.48 |   **1.41** |        −33% | 18 / 2                  |
| `coalesce-2`              | 3.61 / 3.46 / 3.48 / 3.69 / 3.47 |   **3.48** |        +66% | 2 / 200                 |

Размер кеша (Storage tab / `getRDDStorageInfo`): `parsed` - **18.9 MB** в 18 партициях (из 140 MB текста: остались
4 колонки, in-memory columnar формат со сжатием), `counts` - **0.2 MB** в 200 партициях (0.1 MB в 18).

## Пояснения

### cache / persist

После `persist()` в физическом плане вместо `FileScan` + регулярок появляется чтение из памяти:

```text
HashAggregate(keys=[ip], functions=[count(1)])
+- Exchange hashpartitioning(ip, 200)
   +- HashAggregate(keys=[ip], functions=[partial_count(1)])
      +- InMemoryTableScan [ip]
            +- InMemoryRelation [ip, url, status, bytes], StorageLevel(memory, deserialized, 1 replicas)
                  +- Project [regexp_extract(...) AS ip, ... ]
                     +- Filter (isnotnull(value) AND RLIKE(value, ...))
                        +- FileScan text [value]
```

- На представленных данных кэш результатов (`counts`) выгоднее кэша промежуточных данных (`parsed) при любом количестве
  shuffle партиций. Это обусловлено тем, что `counts` необходим 4м action'ам из 7.

### Число партиций

- На текущем окружении разбиение на 200 партиций по умолчанию неэффективно из-за малого объема данных и количества ядер.
- `coalesce(2)` антипример (+66%): использование 2х ядер из 18 доступных приводит к замедлению даже на таком объеме
  данных
- AQE (дефолт spark) сам объединяет мелкие партиции после shuffle (200 → 2 для `counts`) и даёт −33% без ручных
  манипуляций, что уже хорошо, но проигрывает ручной настройке.

### Итог

За счет счет оптимизицаций удалось сократить время выполнения ~в 4 раза (2.10 s -> 0.52 s).
