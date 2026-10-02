package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * (word, [count...]) -> (word, sum). Also used as the combiner.
 */
public class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable total = new LongWritable();

    @Override
    protected void reduce(
            Text key,
            Iterable<LongWritable> values,
            Reducer<Text, LongWritable, Text, LongWritable>.Context context
    ) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        total.set(sum);
        context.write(key, total);
    }
}
