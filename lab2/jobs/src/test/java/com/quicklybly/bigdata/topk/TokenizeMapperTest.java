package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TokenizeMapperTest {

    private final List<String> output = new ArrayList<>();
    private Mapper<LongWritable, Text, Text, LongWritable>.Context context;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        context = mock(Mapper.Context.class);
        // the mapper reuses its Text, so the pair is copied at call time instead of captured
        doAnswer(inv -> output.add(inv.getArgument(0) + "=" + inv.getArgument(1)))
                .when(context).write(any(), any());
    }

    @Test
    void emitsOnePairPerWord() throws Exception {
        var mapper = mapper(false);

        mapper.map(new LongWritable(0), new Text("The whale, the sea!"), context);

        assertThat(output).containsExactly("the=1", "whale=1", "the=1", "sea=1");
    }

    @Test
    void emitsNothingForLineWithoutWords() throws Exception {
        var mapper = mapper(false);

        mapper.map(new LongWritable(0), new Text("  *** 1603, --  "), context);

        assertThat(output).isEmpty();
    }

    @Test
    void emitsLemmasWhenEnabled() throws Exception {
        var mapper = mapper(true);

        mapper.map(new LongWritable(0), new Text("The whales were swimming"), context);

        assertThat(output).containsExactly("the=1", "whale=1", "be=1", "swim=1");
    }

    private TokenizeMapper mapper(boolean lemmatize) {
        var conf = new Configuration(false);
        conf.setBoolean(TokenizeMapper.LEMMATIZE_PROPERTY, lemmatize);
        when(context.getConfiguration()).thenReturn(conf);

        var mapper = new TokenizeMapper();
        mapper.setup(context);
        return mapper;
    }
}
