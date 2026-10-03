package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.Tokenizer;
import com.quicklybly.bigdata.topk.utils.WordCount;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * Usage: java -cp topk-jobs.jar com.quicklybly.bigdata.topk.LocalTopK input [k] [lemmatize]
 */
public final class LocalTopK {

    public record Result(List<WordCount> top, long totalWords, int uniqueWords) {
    }

    private LocalTopK() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: LocalTopK input [k] [lemmatize]");
            System.exit(2);
        }
        var input = Path.of(args[0]);
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        boolean lemmatize = args.length <= 2 || Boolean.parseBoolean(args[2]);

        long start = System.nanoTime();
        var result = run(input, k, new Tokenizer(lemmatize));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        print(result, System.out);
        System.err.printf("total words=%d, unique words=%d, time=%d ms%n",
                result.totalWords(), result.uniqueWords(), elapsedMs);
    }

    public static Result run(Path input, int k, Tokenizer tokenizer) throws IOException {
        Map<String, Long> counts = new HashMap<>();
        long total = 0;
        try (Stream<Path> files = Files.list(input)) {
            for (var file : files.filter(Files::isRegularFile).sorted().toList()) {
                for (var line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    for (var word : tokenizer.tokenize(line).toList()) {
                        counts.merge(word, 1L, Long::sum);
                        total++;
                    }
                }
            }
        }

        var heap = new PriorityQueue<>(WordCount.WORST_FIRST);
        counts.forEach((word, count) -> {
            heap.add(new WordCount(word, count));
            if (heap.size() > k) {
                heap.poll();
            }
        });
        return new Result(heap.stream().sorted(WordCount.BEST_FIRST).toList(), total, counts.size());
    }

    static void print(Result result, PrintStream out) {
        for (var wc : result.top()) {
            out.println(wc.word() + "\t" + wc.count());
        }
    }
}
