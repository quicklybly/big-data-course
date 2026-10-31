# ДЗ 4. Введение в Spark

**Цель**: понять разницу между RDD и Dataset/DataFrame-подходами в Spark.

**Задание**:

- запустить Spark локально или на hadoop-кластере, без Hadoop-кластера).
- Взять лог-файл/CSV и выполнить одну задачу (например, подсчёт числа событий по IP-адресам) двумя способами:
- через RDD (map / filter / reduceByKey );
- через DataFrame/Dataset (select , groupBy , count ).

**Форма отчетности:** результатом должен быть отчет, который содержит описание двух видов реализаций (rdd и api
dataframe) и
объяснение, чем концептуально различаются эти два вида реализаций. Необходимо сравнить полученные результаты, также
указать операции, которые относятся к action и transformation.

**Критерии (max 6 б.)**: Spark запущен локально, задача корректно реализована через RDD и DataFrame/Dataset API,
результаты
выполнения обоих вариантов идентичны, продемонстрировано понимание архитектурных и синтаксических различий подходов, код
работает без ошибок.

## Инфраструктура

Spark 4.3.0 запускается в локальном режиме.

```shell
cd lab4/spark
./gradlew generateLog   # генерирует apache combined логи, на 1kk строк
```

Spark UI доступен на http://localhost:4040.

## Задача

Посчитать число запросов по каждому IP, убрать невалидные строки (не начинающиеся с IPv4).

## Реализация через RDD

[RddIpCount.java](spark/src/main/java/com/quicklybly/bigdata/spark/RddIpCount.java)

```java
JavaRDD<String> lines = sc.textFile(input);
JavaRDD<String> valid = lines.filter(line -> LogFormat.IP_PATTERN.matcher(line).find());
JavaPairRDD<String, Long> ones = valid.mapToPair(line -> new Tuple2<>(extractIp(line), 1L));
JavaPairRDD<String, Long> counts = ones.reduceByKey(Long::sum).cache();
```

RDD - более императивный подход, каждая операция это java-объект и лямбды.

Lineage (`counts.toDebugString()`):

```text
(5) ShuffledRDD[4] at reduceByKey at RddIpCount.java:34 [Memory Deserialized 1x Replicated]
 +-(5) MapPartitionsRDD[3] at mapToPair at RddIpCount.java:33 [Memory Deserialized 1x Replicated]
    |  MapPartitionsRDD[2] at filter at RddIpCount.java:32 [Memory Deserialized 1x Replicated]
    |  lab4/data/access.log MapPartitionsRDD[1] at textFile at RddIpCount.java:31 [Memory Deserialized 1x Replicated]
    |  lab4/data/access.log HadoopRDD[0] at textFile at RddIpCount.java:31 [Memory Deserialized 1x Replicated]
```

## Реализация через DataFrame

[DataFrameIpCount.java](spark/src/main/java/com/quicklybly/bigdata/spark/DataFrameIpCount.java)

```java
Dataset<Row> lines = spark.read().text(input);
Dataset<Row> valid = lines.filter(col("value").rlike(LogFormat.IP_REGEX));
Dataset<Row> ips = valid.select(regexp_extract(col("value"), LogFormat.IP_REGEX, 1).as("ip"));
Dataset<Row> counts = ips.groupBy("ip").count().cache();
```

Более декларативный подход, похож на sql dsl, spark может оптимизировать получившийся код.

Физический план (`counts.explain(true)`, сокращён):

```text
HashAggregate(keys=[ip#2], functions=[count(1)])
+- Exchange hashpartitioning(ip#2, 200)
   +- HashAggregate(keys=[ip#2], functions=[partial_count(1)])
      +- Project [regexp_extract(value#0, ^(\d{1,3}(?:\.\d{1,3}){3}) , 1) AS ip#2]
         +- Filter (isnotnull(value#0) AND RLIKE(value#0, ^(\d{1,3}(?:\.\d{1,3}){3}) ))
            +- FileScan text [value#0] ... PushedFilters: [IsNotNull(value)]
```

Оптимизатор сам добавил то, чего нет в коде:

- `partial_count` до shuffle - предагрегация на каждой партиции.
- фильтр `isnotnull` и его пропагация (`PushedFilters`).

## Сравнение результатов

Обе реализации дают 990 131 валидную строку и 5000 уникальных IP. Топ-10:

| RDD                    | DataFrame              |
|------------------------|------------------------|
| `140.13.174.145 57901` | `140.13.174.145 57901` |
| `180.241.70.111 14910` | `180.241.70.111 14910` |
| `129.23.231.99  10544` | `129.23.231.99  10544` |
| `86.97.70.23     8473` | `86.97.70.23     8473` |
| `66.195.200.170  7138` | `66.195.200.170  7138` |
| `10.38.111.236   6238` | `10.38.111.236   6238` |
| `53.238.98.245   5464` | `53.238.98.245   5464` |
| `197.38.152.145  4914` | `197.38.152.145  4914` |
| `40.64.211.226   4647` | `40.64.211.226   4647` |
| `22.40.150.21    4325` | `22.40.150.21    4325` |

Результаты совпадают, полные файлы - `lab4/data/out/rdd` и `lab4/data/out/df`.

Время выполнения: RDD ~1.0s, DataFrame ~3.4s, dataframe проигрывает, тк у него больше накладных расходов на старте -
построение и оптимизация плана, кодогенерация и тд, на больших объемах результат может быть другой.

## Transformations и actions

Transformations ленивые: возвращают новый RDD/DataFrame и только дописывают шаг в lineage/план. Actions запускают
вычисление (job) и возвращают результат драйверу или пишут его на диск. Аналог java streams - intermediate и terminal
operations.

|           | Transformations                                                                                                     | Actions                                     |
|-----------|---------------------------------------------------------------------------------------------------------------------|---------------------------------------------|
| RDD       | `textFile`, `filter`, `mapToPair`, `reduceByKey` (wide, shuffle), `values`, `sortByKey`, `map`, `coalesce`, `cache` | `count`, `reduce`, `take`, `saveAsTextFile` |
| DataFrame | `read().text`, `filter`, `select`, `groupBy().count()`, `agg`, `orderBy`, `coalesce`, `cache`                       | `count`, `first`, `show`, `write().csv`     |
