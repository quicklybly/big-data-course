package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TopKDriverTest {

    @Test
    void runsBothJobs(@TempDir Path dir) throws Exception {
        Path input = Files.createDirectory(dir.resolve("in"));
        Files.writeString(input.resolve("a.txt"), "the whale the sea\nthe whale ahab\n");
        Files.writeString(input.resolve("b.txt"), "the sea the whale\nstarbuck\n");
        Path output = dir.resolve("out");

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setBoolean(TokenizeMapper.LEMMATIZE_PROPERTY, false);

        // input output k reducers
        int exitCode = ToolRunner.run(conf, new TopKDriver(),
                new String[]{input.toUri().toString(), output.toUri().toString(), "3", "2"});

        assertThat(exitCode).isZero();
        assertThat(Files.readAllLines(output.resolve("part-r-00000")))
                .containsExactly("the\t5", "whale\t3", "sea\t2");
    }
}
