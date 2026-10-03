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
    void keepsContractionsWithTypographicApostrophe() {
        var tokenizer = new Tokenizer(false);

        assertThat(tokenizer.tokenize("He\u2019s sure, don\u2019t move"))
                .containsExactly("he's", "sure", "don't", "move");
    }

    @Test
    void lemmatizesClitics() {
        var tokenizer = new Tokenizer(true);

        assertThat(tokenizer.tokenize("He\u2019s sure the whale\u2019s tail won\u2019t move, I\u2019ll go"))
                .containsExactly("he", "be", "sure", "the", "whale", "tail", "will", "not", "move", "i", "will", "go");
    }

    @Test
    void treatsApostropheDAsElidedEExceptAfterPronoun() {
        var tokenizer = new Tokenizer(true);

        assertThat(tokenizer.tokenize("I\u2019d say he call\u2019d and kill\u2019d it"))
                .containsExactly("i", "would", "say", "he", "call", "and", "kill", "it");
    }

    @Test
    void lowercasesProperNounsWhenLemmatizing() {
        var tokenizer = new Tokenizer(true);

        assertThat(tokenizer.tokenize("Ahab saw the whale")).containsExactly("ahab", "see", "the", "whale");
    }

    @Test
    void stripsDiacriticsAndLigatures() {
        var tokenizer = new Tokenizer(false);

        assertThat(tokenizer.tokenize("Borís met Sónya and Cæsar at the Phœnix"))
                .containsExactly("boris", "met", "sonya", "and", "caesar", "at", "the", "phoenix");
    }

    @Test
    void emptyLine() {
        assertThat(new Tokenizer(true).tokenize("  *** ")).isEmpty();
    }
}
