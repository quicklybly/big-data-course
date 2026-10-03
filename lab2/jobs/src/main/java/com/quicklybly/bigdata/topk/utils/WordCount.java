package com.quicklybly.bigdata.topk.utils;

import java.util.Comparator;

public record WordCount(String word, long count) {

    public static final Comparator<WordCount> BEST_FIRST = Comparator
            .comparingLong(WordCount::count).reversed()
            .thenComparing(WordCount::word);

    public static final Comparator<WordCount> WORST_FIRST = BEST_FIRST.reversed();
}
