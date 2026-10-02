package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.WordCount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * (total, [word...]) from all mappers' local top-K -> global top-K (word, total), emitted in cleanup()
 */
public class TopKReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    private final PriorityQueue<WordCount> heap = new PriorityQueue<>(WordCount.WORST_FIRST);
    private int k;

    @Override
    protected void setup(Reducer<LongWritable, Text, Text, LongWritable>.Context context) {
        k = context.getConfiguration().getInt(TopKDriver.K_PROPERTY, 10);
    }

    @Override
    protected void reduce(
            LongWritable key,
            Iterable<Text> values,
            Reducer<LongWritable, Text, Text, LongWritable>.Context context
    ) {
        for (var value : values) {
            heap.add(new WordCount(value.toString(), key.get()));
            if (heap.size() > k) {
                heap.poll();
            }
        }
    }

    @Override
    protected void cleanup(Reducer<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        var word = new Text();
        var count = new LongWritable();
        for (var wc : heap.stream().sorted(WordCount.BEST_FIRST).toList()) {
            word.set(wc.word());
            count.set(wc.count());
            context.write(word, count);
        }
    }
}
