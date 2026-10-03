package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.Tokenizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class LocalTopKTest {

    @Test
    void matchesMapReduceResult(@TempDir Path dir) throws Exception {
        // the same corpus as TopKDriverTest.runsBothJobs
        Files.writeString(dir.resolve("a.txt"), "the whale the sea\nthe whale ahab\n");
        Files.writeString(dir.resolve("b.txt"), "the sea the whale\nstarbuck\n");

        var result = LocalTopK.run(dir, 3, new Tokenizer(false));

        assertThat(lines(result)).containsExactly("the\t5", "whale\t3", "sea\t2");
        assertThat(result.totalWords()).isEqualTo(12);
        assertThat(result.uniqueWords()).isEqualTo(5);
    }

    @Test
    void breaksTiesAlphabetically(@TempDir Path dir) throws Exception {
        // the same corpus as TopKDriverTest.breaksTiesAlphabetically
        Files.writeString(dir.resolve("a.txt"), "the the pear kiwi\n");
        Files.writeString(dir.resolve("b.txt"), "fig apple banana\n");

        var result = LocalTopK.run(dir, 4, new Tokenizer(false));

        assertThat(lines(result)).containsExactly("the\t2", "apple\t1", "banana\t1", "fig\t1");
    }

    private static String[] lines(LocalTopK.Result result) {
        var out = new ByteArrayOutputStream();
        LocalTopK.print(result, new PrintStream(out, true));
        return out.toString().lines().toArray(String[]::new);
    }
}
