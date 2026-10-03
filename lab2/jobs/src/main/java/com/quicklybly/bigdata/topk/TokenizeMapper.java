package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.Tokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * (offset, line) -> (word, 1)
 */
public class TokenizeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public static final String LEMMATIZE_PROPERTY = "topk.lemmatize";

    private static final LongWritable ONE = new LongWritable(1);
    private final Text word = new Text();

    private Tokenizer tokenizer;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
        tokenizer = new Tokenizer(context.getConfiguration().getBoolean(LEMMATIZE_PROPERTY, true));
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, Text, LongWritable>.Context context
    ) throws IOException, InterruptedException {
        Iterator<String> it = tokenizer.tokenize(value.toString()).iterator();
        while (it.hasNext()) {
            word.set(it.next());
            context.write(word, ONE);
        }
    }
}
