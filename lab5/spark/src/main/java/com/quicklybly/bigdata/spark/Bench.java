package com.quicklybly.bigdata.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public final class Bench {

    private Bench() {
    }

    public static List<String> parseModes(String arg, List<String> all) {
        if (arg.equals("all")) {
            return all;
        }
        List<String> modes = Arrays.asList(arg.split(","));
        for (String mode : modes) {
            if (!all.contains(mode)) {
                throw new IllegalArgumentException("Unknown mode " + mode + ", expected one of " + all);
            }
        }
        return modes;
    }

    public static void run(List<String> modes, int runs, Function<String, String> job) {
        System.out.println("Warm-up (" + modes.get(0) + ")...");
        job.apply(modes.get(0));

        Map<String, List<Double>> times = new LinkedHashMap<>();
        Map<String, String> results = new LinkedHashMap<>();
        for (int round = 1; round <= runs; round++) {
            for (String mode : modes) {
                long start = System.nanoTime();
                String result = job.apply(mode);
                double seconds = (System.nanoTime() - start) / 1e9;
                times.computeIfAbsent(mode, m -> new ArrayList<>()).add(seconds);
                results.put(mode, result);
                System.out.printf("round %d  %-22s %6.2f s  %s%n", round, mode, seconds, result);
            }
        }

        System.out.println();
        System.out.println("| mode | runs, s | median, s | result |");
        System.out.println("|------|---------|-----------|--------|");
        for (String mode : modes) {
            List<Double> t = times.get(mode);
            List<Double> sorted = t.stream().sorted().toList();
            double median = sorted.get(sorted.size() / 2);
            String all = String.join(" / ", t.stream().map(x -> String.format("%.2f", x)).toList());
            System.out.printf("| `%s` | %s | **%.2f** | %s |%n", mode, all, median, results.get(mode));
        }
        if (results.values().stream().distinct().count() != 1) {
            throw new IllegalStateException("Modes produced different results: " + results);
        }
        System.out.println("All modes produced identical results.");
    }

    public static void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
