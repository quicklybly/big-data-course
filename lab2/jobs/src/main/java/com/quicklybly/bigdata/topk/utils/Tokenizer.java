package com.quicklybly.bigdata.topk.utils;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.process.Morphology;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.text.Normalizer;
import java.util.List;
import java.util.Locale;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class Tokenizer {

    public static final String TAGGER_MODEL =
            "edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger";

    private static final Pattern WORD = Pattern.compile("[A-Za-z]+(?:'[A-Za-z]+)*");

    private static final Pattern COMBINING_MARKS = Pattern.compile("\\p{M}+");

    // the tagger is trained on Penn Treebank tokens, where clitics are separate: he's -> he 's, don't -> do n't
    private static final Pattern CLITIC = Pattern.compile("(?i)(.+?)(n't|'s|'ll|'re|'ve|'d|'m)");

    // possessive 's, as in whale's
    private static final String POSSESSIVE_TAG = "POS";

    private final MaxentTagger tagger;
    private final Morphology morphology;

    public Tokenizer(boolean lemmatize) {
        this.tagger = lemmatize ? new MaxentTagger(TAGGER_MODEL) : null;
        this.morphology = lemmatize ? new Morphology() : null;
    }

    public Stream<String> tokenize(String line) {
        List<String> words = WORD.matcher(normalize(line)).results()
                .map(MatchResult::group)
                .toList();

        if (tagger == null || words.isEmpty()) {
            return words.stream().map(w -> w.toLowerCase(Locale.ROOT));
        }

        List<TaggedWord> tagged = tagger.tagSentence(words.stream()
                .flatMap(Tokenizer::splitClitic)
                .map(Word::new)
                .toList());
        return tagged.stream()
                .filter(tw -> !POSSESSIVE_TAG.equals(tw.tag()))
                // the lemmatizer keeps the case of I and proper nouns
                .map(tw -> morphology.lemma(tw.word(), tw.tag(), true).toLowerCase(Locale.ROOT));
    }

    /**
     * Brings the text to the ASCII letters WORD matches, otherwise words get torn apart: Gutenberg texts use the
     * typographic apostrophe (don’t -> don + t), War and Peace spells names with accents (Borís -> bor + s),
     * old texts use ligatures (Cæsar -> c + sar).
     */
    static String normalize(String line) {
        String decomposed = Normalizer.normalize(line.replace('’', '\''), Normalizer.Form.NFD);
        return COMBINING_MARKS.matcher(decomposed).replaceAll("")
                .replace("æ", "ae").replace("Æ", "Ae")
                .replace("œ", "oe").replace("Œ", "Oe");
    }

    private static Stream<String> splitClitic(String word) {
        var m = CLITIC.matcher(word);
        return m.matches() ? Stream.of(m.group(1), m.group(2)) : Stream.of(word);
    }
}
