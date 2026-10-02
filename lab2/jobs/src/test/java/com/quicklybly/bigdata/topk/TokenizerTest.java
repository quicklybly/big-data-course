package com.quicklybly.bigdata.topk;

import com.quicklybly.bigdata.topk.utils.Tokenizer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TokenizerTest {

    @Test
    void splitsAndLowercasesWithoutLemmatization() {
        var tokenizer = new Tokenizer(false);

        assertEquals(List.of("the", "whales", "o'er", "the", "sea"),
                tokenizer.tokenize("The whales, o'er the sea!").toList());
    }

    @Test
    void lemmatizes() {
        var tokenizer = new Tokenizer(true);

        assertEquals(List.of("the", "whale", "be", "swim", "in", "the", "sea"),
                tokenizer.tokenize("The whales were swimming in the sea.").toList());
    }

    @Test
    void emptyLine() {
        assertEquals(List.of(), new Tokenizer(true).tokenize("  *** ").toList());
    }
}
