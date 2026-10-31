package com.quicklybly.bigdata.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class DataFrameIpCount {

    public static void main(String[] args) throws IOException {
        String input = args[0];
        String output = args[1];

        try (SparkSession spark = SparkSession.builder()
                .appName("lab4-dataframe")
                .master("local[*]")
                .getOrCreate()) {
            long start = System.nanoTime();

            Dataset<Row> lines = spark.read().text(input);
            Dataset<Row> valid = lines.filter(col("value").rlike(LogFormat.IP_REGEX));
            Dataset<Row> ips = valid.select(regexp_extract(col("value"), LogFormat.IP_REGEX, 1).as("ip"));
            Dataset<Row> counts = ips.groupBy("ip").count().cache();

            System.out.println("Plan:");
            counts.explain(true);

            long distinctIps = counts.count();
            long validLines = counts.agg(sum("count")).first().getLong(0);
            System.out.println("Distinct IPs: " + distinctIps);
            System.out.println("Valid lines:  " + validLines);

            System.out.println("Top 10 IPs:");
            counts.orderBy(desc("count")).show(10, false);

            counts.orderBy("ip")
                    .coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .csv(output);

            System.out.printf("Done in %.1f s, result in %s%n", (System.nanoTime() - start) / 1e9, output);
        }
    }
}
