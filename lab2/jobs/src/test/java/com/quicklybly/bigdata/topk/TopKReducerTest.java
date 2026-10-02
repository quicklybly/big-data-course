package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
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
import static org.mockito.Mockito.when;

class TopKReducerTest {

    private final List<String> output = new ArrayList<>();
    private Reducer<LongWritable, Text, Text, LongWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        context = mock(Reducer.Context.class);
        doAnswer(inv -> output.add(inv.getArgument(0) + "=" + inv.getArgument(1)))
                .when(context).write(any(), any());
    }

    @Test
    void emitsGlobalTopKInRankingOrder() throws Exception {
        var reducer = reducer(3);

        // keys arrive in descending order after the shuffle
        reducer.reduce(new LongWritable(100), texts("the"), context);
        reducer.reduce(new LongWritable(40), texts("whale"), context);
        reducer.reduce(new LongWritable(7), texts("ahab"), context);
        reducer.reduce(new LongWritable(5), texts("sea"), context);
        reducer.cleanup(context);

        assertThat(output).containsExactly("the=100", "whale=40", "ahab=7");
    }

    @Test
    void countsKAcrossWordsOfSameCount() throws Exception {
        var reducer = reducer(3);

        // words with the same count come in one call, in no particular order
        reducer.reduce(new LongWritable(10), texts("the"), context);
        reducer.reduce(new LongWritable(3), texts("c", "a", "b"), context);
        reducer.cleanup(context);

        assertThat(output).containsExactly("the=10", "a=3", "b=3");
    }

    @Test
    void emitsAllWhenFewerThanK() throws Exception {
        var reducer = reducer(10);

        reducer.reduce(new LongWritable(5), texts("sea"), context);
        reducer.cleanup(context);

        assertThat(output).containsExactly("sea=5");
    }

    private TopKReducer reducer(int k) {
        var conf = new Configuration(false);
        conf.setInt(TopKDriver.K_PROPERTY, k);
        when(context.getConfiguration()).thenReturn(conf);

        var reducer = new TopKReducer();
        reducer.setup(context);
        return reducer;
    }

    // a single reused Text, as Hadoop does when iterating values
    private static Iterable<Text> texts(String... words) {
        var text = new Text();
        return () -> java.util.Arrays.stream(words).map(w -> {
            text.set(w);
            return text;
        }).iterator();
    }
}
