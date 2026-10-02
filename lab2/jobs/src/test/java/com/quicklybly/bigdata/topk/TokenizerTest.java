package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.Tokenizer;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;


class TokenizerTest {

    @Test
    void splitsAndLowercasesWithoutLemmatization() {
        var tokenizer = new Tokenizer(false);

        assertThat(tokenizer.tokenize("The whales, o'er the sea!"))
                .containsExactly("the", "whales", "o'er", "the", "sea");
    }

    @Test
    void lemmatizes() {
        var tokenizer = new Tokenizer(true);

        assertThat(tokenizer.tokenize("The whales were swimming in the sea."))
                .containsExactly("the", "whale", "be", "swim", "in", "the", "sea");
    }

    @Test
    void emptyLine() {
        assertThat(new Tokenizer(true).tokenize("  *** ")).isEmpty();
    }
}
