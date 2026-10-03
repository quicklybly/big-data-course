package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TopKMapperTest {

    private final List<String> output = new ArrayList<>();
    private Mapper<Text, LongWritable, CountWordKey, NullWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        context = mock(Mapper.Context.class);
        doAnswer(inv -> output.add(inv.getArgument(0).toString()))
                .when(context).write(any(), any());
    }

    @Test
    void keepsKMostFrequentWords() throws Exception {
        var mapper = mapper(3);

        var word = new Text();
        var count = new LongWritable();
        map(mapper, word, count, "sea", 7);
        map(mapper, word, count, "the", 100);
        map(mapper, word, count, "whale", 40);
        map(mapper, word, count, "see", 7);
        mapper.cleanup(context);

        assertThat(output).containsExactlyInAnyOrder("the=100", "whale=40", "sea=7");
    }

    private TopKMapper mapper(int k) {
        var conf = new Configuration(false);
        conf.setInt(TopKDriver.K_PROPERTY, k);
        when(context.getConfiguration()).thenReturn(conf);

        var mapper = new TopKMapper();
        mapper.setup(context);
        return mapper;
    }

    private void map(TopKMapper mapper, Text word, LongWritable count, String w, long c) {
        word.set(w);
        count.set(c);
        mapper.map(word, count, context);
    }
}
