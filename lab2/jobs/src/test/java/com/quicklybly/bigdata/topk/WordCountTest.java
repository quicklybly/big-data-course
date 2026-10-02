package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.WordCount;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WordCountTest {

    private final List<WordCount> words = List.of(
            new WordCount("b", 3), new WordCount("the", 100), new WordCount("a", 3), new WordCount("sea", 5));

    @Test
    void bestFirstSortsByCountDescThenWord() {
        assertThat(words.stream().sorted(WordCount.BEST_FIRST).map(WordCount::word))
                .containsExactly("the", "sea", "a", "b");
    }

    @Test
    void worstFirstIsReverse() {
        assertThat(words.stream().sorted(WordCount.WORST_FIRST).map(WordCount::word))
                .containsExactly("b", "a", "sea", "the");
    }
}
