package com.quicklybly.bigdata.topk;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Composite shuffle key (count, word) ordered by count desc, then word asc.
 */
public class CountWordKey implements WritableComparable<CountWordKey> {

    private long count;
    private final Text word = new Text();

    public CountWordKey() {
    }

    public CountWordKey(long count, String word) {
        set(count, word);
    }

    public void set(long count, String word) {
        this.count = count;
        this.word.set(word);
    }

    public long getCount() {
        return count;
    }

    public Text getWord() {
        return word;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        word.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        word.readFields(in);
    }

    @Override
    public int compareTo(CountWordKey other) {
        int byCount = Long.compare(other.count, count);
        return byCount != 0 ? byCount : word.compareTo(other.word);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CountWordKey other && count == other.count && word.equals(other.word);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(count) + word.hashCode();
    }

    @Override
    public String toString() {
        return word + "=" + count;
    }
}
