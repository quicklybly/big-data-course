package com.quicklybly.bigdata.topk.utils;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.util.List;
import java.util.Locale;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class Tokenizer {

    public static final String TAGGER_MODEL =
            "edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger";

    private static final Pattern WORD = Pattern.compile("[A-Za-z]+(?:'[A-Za-z]+)*");

    private final MaxentTagger tagger;
    private final Morphology morphology;

    public Tokenizer(boolean lemmatize) {
        this.tagger = lemmatize ? new MaxentTagger(TAGGER_MODEL) : null;
        this.morphology = lemmatize ? new Morphology() : null;
    }

    public Stream<String> tokenize(String line) {
        List<String> words = WORD.matcher(line).results()
                .map(MatchResult::group)
                .toList();

        if (tagger == null || words.isEmpty()) {
            return words.stream().map(w -> w.toLowerCase(Locale.ROOT));
        }

        List<TaggedWord> tagged = tagger.tagSentence(words.stream().map(Word::new).toList());
        return tagged.stream()
                .map(tw -> morphology.lemma(tw.word(), tw.tag(), true));
    }
}
