package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopKReducerTest {

    private final List<String> output = new ArrayList<>();
    private Reducer<CountWordKey, NullWritable, Text, LongWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        context = mock(Reducer.Context.class);
        doAnswer(inv -> output.add(inv.getArgument(0) + "=" + inv.getArgument(1)))
                .when(context).write(any(), any());
    }

    @Test
    void emitsFirstKKeysInShuffleOrder() throws Exception {
        // keys arrive sorted by count desc, then word asc after the shuffle
        var keys = feed(
                new CountWordKey(100, "the"),
                new CountWordKey(40, "whale"),
                new CountWordKey(7, "ahab"),
                new CountWordKey(5, "sea"));

        reducer(3).run(context);

        assertThat(output).containsExactly("the=100", "whale=40", "ahab=7");
        assertThat(keys).as("stops reading after K keys").hasSize(1);
    }

    @Test
    void cutsTiesAtK() throws Exception {
        feed(
                new CountWordKey(10, "the"),
                new CountWordKey(3, "a"),
                new CountWordKey(3, "b"),
                new CountWordKey(3, "c"));

        reducer(3).run(context);

        assertThat(output).containsExactly("the=10", "a=3", "b=3");
    }

    @Test
    void emitsAllWhenFewerThanK() throws Exception {
        feed(new CountWordKey(5, "sea"));

        reducer(10).run(context);

        assertThat(output).containsExactly("sea=5");
    }

    private TopKReducer reducer(int k) {
        var conf = new Configuration(false);
        conf.setInt(TopKDriver.K_PROPERTY, k);
        when(context.getConfiguration()).thenReturn(conf);
        return new TopKReducer();
    }

    private Deque<CountWordKey> feed(CountWordKey... keys) throws Exception {
        var remaining = new ArrayDeque<>(List.of(keys));
        var current = new CountWordKey[1];
        when(context.nextKey()).thenAnswer(inv -> (current[0] = remaining.poll()) != null);
        when(context.getCurrentKey()).thenAnswer(inv -> current[0]);
        when(context.getValues()).thenReturn(List.of(NullWritable.get()));
        return remaining;
    }
}
