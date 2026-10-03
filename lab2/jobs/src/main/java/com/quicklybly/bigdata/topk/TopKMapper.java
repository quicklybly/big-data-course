package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.WordCount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * (word, total) -> local top-K as ((total, word), null), emitted in cleanup()
 */
public class TopKMapper extends Mapper<Text, LongWritable, CountWordKey, NullWritable> {

    private final PriorityQueue<WordCount> heap = new PriorityQueue<>(WordCount.WORST_FIRST);
    private int k;

    @Override
    protected void setup(Mapper<Text, LongWritable, CountWordKey, NullWritable>.Context context) {
        k = context.getConfiguration().getInt(TopKDriver.K_PROPERTY, 10);
    }

    @Override
    protected void map(
            Text key,
            LongWritable value,
            Mapper<Text, LongWritable, CountWordKey, NullWritable>.Context context
    ) {
        heap.add(new WordCount(key.toString(), value.get()));
        if (heap.size() > k) {
            heap.poll();
        }
    }

    @Override
    protected void cleanup(Mapper<Text, LongWritable, CountWordKey, NullWritable>.Context context)
            throws IOException, InterruptedException {
        var key = new CountWordKey();
        for (var wc : heap) {
            key.set(wc.count(), wc.word());
            context.write(key, NullWritable.get());
        }
    }
}
