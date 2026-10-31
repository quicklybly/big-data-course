package com.quicklybly.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Stream;

public class RddIpCount {

    public static void main(String[] args) throws IOException {
        String input = args[0];
        String output = args[1];
        deleteRecursively(Path.of(output));

        SparkConf conf = new SparkConf()
                .setAppName("lab4-rdd")
                .setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            long start = System.nanoTime();

            JavaRDD<String> lines = sc.textFile(input);
            JavaRDD<String> valid = lines.filter(line -> LogFormat.IP_PATTERN.matcher(line).find());
            JavaPairRDD<String, Long> ones = valid.mapToPair(line -> new Tuple2<>(extractIp(line), 1L));
            JavaPairRDD<String, Long> counts = ones.reduceByKey(Long::sum).cache();

            System.out.println("Lineage:");
            System.out.println(counts.toDebugString());

            long distinctIps = counts.count();
            long validLines = counts.values().reduce(Long::sum);
            System.out.println("Distinct IPs: " + distinctIps);
            System.out.println("Valid lines:  " + validLines);

            List<Tuple2<Long, String>> top = counts
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .take(10);
            System.out.println("Top 10 IPs:");
            top.forEach(t -> System.out.printf("  %-16s %d%n", t._2(), t._1()));

            counts.sortByKey()
                    .map(t -> t._1() + "," + t._2())
                    .coalesce(1)
                    .saveAsTextFile(output);

            System.out.printf("Done in %.1f s, result in %s%n", (System.nanoTime() - start) / 1e9, output);
        }
    }

    private static String extractIp(String line) {
        Matcher matcher = LogFormat.IP_PATTERN.matcher(line);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Not a log line: " + line);
        }
        return matcher.group(1);
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
