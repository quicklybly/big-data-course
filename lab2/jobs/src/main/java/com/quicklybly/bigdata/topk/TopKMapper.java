package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * (word, total) -> local top-K as (total, word), emitted in cleanup()
 */
public class TopKMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

    private record WordCount(String word, long count) {
    }

    private static final Comparator<WordCount> WORST_FIRST = Comparator
            .comparingLong(WordCount::count)
            .thenComparing(WordCount::word, Comparator.reverseOrder());

    private final PriorityQueue<WordCount> heap = new PriorityQueue<>(WORST_FIRST);
    private int k;

    @Override
    protected void setup(Mapper<Text, LongWritable, LongWritable, Text>.Context context) {
        k = context.getConfiguration().getInt(TopKDriver.K_PROPERTY, 10);
    }

    @Override
    protected void map(
            Text key,
            LongWritable value,
            Mapper<Text, LongWritable, LongWritable, Text>.Context context
    ) {
        heap.add(new WordCount(key.toString(), value.get()));
        if (heap.size() > k) {
            heap.poll();
        }
    }

    @Override
    protected void cleanup(Mapper<Text, LongWritable, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {
        var count = new LongWritable();
        var word = new Text();
        for (var wc : heap) {
            count.set(wc.count());
            word.set(wc.word());
            context.write(count, word);
        }
    }
}
