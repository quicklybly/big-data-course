package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ((total, word), null) from all mappers' local top-K -> global top-K (word, total).
 * The shuffle already sorts keys by count desc, then word asc, so the first K keys are the answer.
 */
public class TopKReducer extends Reducer<CountWordKey, NullWritable, Text, LongWritable> {

    private int k;
    private int emitted;
    private final LongWritable count = new LongWritable();

    @Override
    protected void setup(Reducer<CountWordKey, NullWritable, Text, LongWritable>.Context context) {
        k = context.getConfiguration().getInt(TopKDriver.K_PROPERTY, 10);
    }

    @Override
    protected void reduce(
            CountWordKey key,
            Iterable<NullWritable> values,
            Reducer<CountWordKey, NullWritable, Text, LongWritable>.Context context
    ) throws IOException, InterruptedException {
        count.set(key.getCount());
        context.write(key.getWord(), count);
        emitted++;
    }

    /**
     * Stops reading input after K keys instead of calling reduce() for the rest.
     */
    @Override
    public void run(Reducer<CountWordKey, NullWritable, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        setup(context);
        try {
            while (emitted < k && context.nextKey()) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
        } finally {
            cleanup(context);
        }
    }
}
