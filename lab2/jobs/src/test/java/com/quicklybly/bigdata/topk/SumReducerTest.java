package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class SumReducerTest {

    private final List<String> output = new ArrayList<>();
    private final SumReducer reducer = new SumReducer();
    private Reducer<Text, LongWritable, Text, LongWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        context = mock(Reducer.Context.class);
        // the reducer reuses its LongWritable, so the pair is copied at call time instead of captured
        doAnswer(inv -> output.add(inv.getArgument(0) + "=" + inv.getArgument(1)))
                .when(context).write(any(), any());
    }

    @Test
    void sums() throws Exception {
        reducer.reduce(new Text("sea"), longs(2), context);
        reducer.reduce(new Text("whale"), longs(4, 4), context);
        reducer.reduce(new Text("the"), longs(Integer.MAX_VALUE, Integer.MAX_VALUE), context);

        assertThat(output).containsExactly("sea=2", "whale=8", "the=" + 2L * Integer.MAX_VALUE);
    }

    private static List<LongWritable> longs(long... values) {
        var list = new ArrayList<LongWritable>();
        for (long v : values) {
            list.add(new LongWritable(v));
        }
        return list;
    }
}
