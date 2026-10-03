package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CountWordKeyTest {

    @Test
    void sortsByCountDescThenWordAsc() {
        var keys = new ArrayList<>(List.of(
                new CountWordKey(3, "b"), new CountWordKey(100, "the"),
                new CountWordKey(3, "a"), new CountWordKey(5, "sea")));

        // the comparator the shuffle uses for this key class
        WritableComparator shuffleOrder = WritableComparator.get(CountWordKey.class);
        keys.sort(shuffleOrder::compare);

        assertThat(keys).map(CountWordKey::toString).containsExactly("the=100", "sea=5", "a=3", "b=3");
    }

    @Test
    void survivesSerialization() throws Exception {
        var out = new DataOutputBuffer();
        new CountWordKey(42, "whale").write(out);

        var in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        var read = new CountWordKey();
        read.readFields(in);

        assertThat(read).isEqualTo(new CountWordKey(42, "whale"));
    }
}
