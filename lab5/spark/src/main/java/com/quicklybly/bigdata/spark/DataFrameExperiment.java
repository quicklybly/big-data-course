package com.quicklybly.bigdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.*;

public class DataFrameExperiment {

    private static final Set<String> DESCRIBED = new HashSet<>();

    private static final List<String> MODES = List.of(
            "baseline",               // no cache, 200 shuffle partitions, AQE off
            "cache-counts",           // cache only the post-shuffle per-IP counts
            "cache-parsed",           // persist the parsed pre-shuffle DataFrame
            "coalesce-18",            // .coalesce(18) right after every groupBy: 200 -> 18 reduce tasks
            "repartition-ip-18",      // .repartition(18, ip) before groupBy(ip): optimizer reuses it, no 2nd exchange
            "shuffle-18",             // spark.sql.shuffle.partitions = 18 (= cores)
            "cache-parsed+shuffle-18",
            "cache-counts+shuffle-18",
            "cache-both+shuffle-18",  // persist parsed AND cache counts
            "aqe",                    // baseline + AQE: coalesces shuffle partitions itself
            "coalesce-2"              // negative case: input squeezed to 2 partitions
    );

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        int runs = Integer.parseInt(args[2]);
        List<String> modes = Bench.parseModes(args[3], MODES);

        try (SparkSession spark = SparkSession.builder()
                .appName("lab5-dataframe")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate()) {
            System.out.println("Cores (defaultParallelism): " + spark.sparkContext().defaultParallelism());
            System.out.println("Input partitions: " + spark.read().text(input).rdd().getNumPartitions());

            Bench.run(modes, runs, mode -> job(spark, input, Path.of(output, mode).toString(), mode));
        }
    }

    private static String job(SparkSession spark, String input, String output, String mode) {
        Bench.deleteRecursively(Path.of(output));
        boolean cacheParsed = mode.startsWith("cache-parsed") || mode.startsWith("cache-both");
        boolean cacheCounts = mode.startsWith("cache-counts") || mode.startsWith("cache-both");
        spark.conf().set("spark.sql.shuffle.partitions", mode.contains("shuffle-18") ? "18" : "200");
        spark.conf().set("spark.sql.adaptive.enabled", String.valueOf(mode.equals("aqe")));

        Dataset<Row> lines = spark.read().text(input);
        if (mode.equals("coalesce-2")) {
            lines = lines.coalesce(2);
        }

        Dataset<Row> parsed = lines
                .filter(col("value").rlike(LogFormat.IP_REGEX))
                .select(
                        regexp_extract(col("value"), LogFormat.LINE_REGEX, 1).as("ip"),
                        regexp_extract(col("value"), LogFormat.LINE_REGEX, 3).as("url"),
                        regexp_extract(col("value"), LogFormat.LINE_REGEX, 4).as("status"),
                        regexp_extract(col("value"), LogFormat.LINE_REGEX, 5).as("bytes"))
                .withColumn("bytes", when(col("bytes").equalTo("-"), lit(null)).otherwise(col("bytes"))
                        .cast("long"));
        if (cacheParsed) {
            parsed = parsed.persist(StorageLevel.MEMORY_ONLY());
        }

        boolean coalesce = mode.equals("coalesce-18");
        if (mode.equals("repartition-ip-18")) {
            parsed = parsed.repartition(18, col("ip"));
        }

        Dataset<Row> counts = parsed.groupBy("ip").count();
        if (coalesce) {
            counts = counts.coalesce(18);
        }
        if (cacheCounts) {
            counts = counts.cache();
        }

        long distinctIps = counts.count();
        long validLines = counts.agg(sum("count")).first().getLong(0);
        List<Row> topIps = counts.orderBy(desc("count")).limit(10).collectAsList();
        Dataset<Row> statuses = parsed.groupBy("status").count();
        List<Row> byStatus = (coalesce ? statuses.coalesce(18) : statuses).collectAsList();
        Dataset<Row> urls = parsed.groupBy("url").count();
        List<Row> topUrls = (coalesce ? urls.coalesce(18) : urls)
                .orderBy(desc("count")).limit(10).collectAsList();
        long totalBytes = parsed.agg(sum("bytes")).first().getLong(0);
        counts.orderBy("ip").coalesce(1).write().mode(SaveMode.Overwrite).csv(output);

        if (DESCRIBED.add(mode)) {
            describe(spark, mode, lines, counts);
        }
        if (cacheParsed) {
            parsed.unpersist(true);
        }
        if (cacheCounts) {
            counts.unpersist(true);
        }
        return String.format("ips=%d lines=%d statuses=%d topUrl=%s bytes=%d top=%s",
                distinctIps, validLines, byStatus.size(), topUrls.get(0).getString(0), totalBytes,
                topIps.get(0).getString(0) + ":" + topIps.get(0).getLong(1));
    }

    private static void describe(SparkSession spark, String mode, Dataset<Row> lines, Dataset<Row> counts) {
        System.out.printf("  [%s] partitions: input=%d, per-IP counts=%d%n",
                mode, lines.rdd().getNumPartitions(), counts.rdd().getNumPartitions());
        for (var info : spark.sparkContext().getRDDStorageInfo()) {
            System.out.printf("  [%s] cached: %s, %d/%d partitions, memory %.1f MB%n", mode, info.name().lines().findFirst().orElse("?"),
                    info.numCachedPartitions(), info.numPartitions(), info.memSize() / 1024.0 / 1024.0);
        }
        System.out.println("  [" + mode + "] physical plan of per-IP counts:");
        System.out.println(counts.queryExecution().executedPlan().treeString());
    }
}
