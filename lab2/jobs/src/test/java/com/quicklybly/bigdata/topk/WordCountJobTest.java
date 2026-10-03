package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;


class WordCountJobTest {

    @Test
    void countsWordsWithCombiner(@TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path input = Files.createDirectory(dir.resolve("in"));
        Files.writeString(input.resolve("a.txt"), "The whale, the WHALE!\nwhales and the sea\n");
        Files.writeString(input.resolve("b.txt"), "the sea\n");
        java.nio.file.Path output = dir.resolve("out");

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setBoolean(TokenizeMapper.LEMMATIZE_PROPERTY, false);

        Job job = Job.getInstance(conf);
        job.setMapperClass(TokenizeMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input.toUri()));
        FileOutputFormat.setOutputPath(job, new Path(output.toUri()));

        assertThat(job.waitForCompletion(false)).isTrue();

        Map<String, Long> counts = new HashMap<>();
        Path part = new Path(output.resolve("part-r-00000").toUri());
        try (var reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(part))) {
            Text word = new Text();
            LongWritable count = new LongWritable();
            while (reader.next(word, count)) {
                counts.put(word.toString(), count.get());
            }
        }

        assertThat(counts).containsExactlyInAnyOrderEntriesOf(
                Map.of("the", 4L, "whale", 2L, "whales", 1L, "and", 1L, "sea", 2L));
    }
}
