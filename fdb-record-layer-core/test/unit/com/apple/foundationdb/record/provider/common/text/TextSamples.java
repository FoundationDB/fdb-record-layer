/*
 * TextSamples.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.common.text;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;

/**
 * Some interesting sample texts.
 */
public class TextSamples {
    // The Angstrom SI unit has a different Unicode codepoint from the
    // letter Å (latin capital A with ring) which itself can also be
    // represented as an A followed by the "combining ring above". This
    // uses the SI unit codepoint within parentheses and the letters
    // character within Ångström's name.
    @SuppressWarnings("checkstyle:avoidEscapedUnicodeCharacters")
    public static final String ANGSTROM = "The Angstrom unit (\u212b) was named after Anders \u00c5ngstr\u00f6m.";

    // This contains a few instances of the æ letter
    public static final String AETHELRED = "According to the encyclopædia, Æthelred the Unræd was king from 966 to 1016.";

    // Says "Hello there." But the characters are written in entirely in the
    // "blackboard" font, which compatibility normalization should collapse
    // to ASCII.
    @SuppressWarnings("checkstyle:avoidEscapedUnicodeCharacters")
    public static final String BLACKBOARD = "\u210d\uD835\uDD56\uD835\uDD5D\uD835\uDD5D\uD835\uDD60 \uD835\uDD65\uD835\uDD59\uD835\uDD56\uD835\uDD63\uD835\uDD56.";

    // Translates to: "apple park"
    // Simplified and traditional representations. There are four characters,
    // no spaces, and two words. A sophisticated, language-aware tokenizer
    // should be able to split it into two words appropriately, but that requires
    // knowledge of the underlying language.
    public static final String CHINESE_SIMPLIFIED = "苹果园区";
    public static final String CHINESE_TRADITIONAL = "蘋果園區";

    // Translates to: "Text tokenization and normalization is really cool!"
    public static final String EMOJIS = "Text tokenization and normalization is 💯🔥👌!";

    // This contains two stylistic ligatures that unicode normalization will split up.
    @SuppressWarnings("checkstyle:avoidEscapedUnicodeCharacters")
    public static final String FIRE = "Who ﬆarted the \ufb01re?";

    // Translates to: "After two Napoleons, France received a Thiers/third."
    // This is a pun playing on the fact that following the exile of
    // Napoléon III (really only the second Emperor of the French to call
    // himself Napoléon), the Presidency of France went to Adolphe Thiers,
    // whose last name is homophonous with one French word for "third".
    // This contains several diacritical marks that, in this normalized form,
    // have all been combined with their letter.
    public static final String FRENCH = Normalizer.normalize("Après deux Napoléons, France a reçu un Thiers.", Normalizer.Form.NFC);

    // Some German text that contains (1) a few umlauts (here combined into the
    // character), (2) some compound words like "Nationalmannschaft" that would
    // be split by a language aware tokenizer, and (3) an "ß" character that
    // could theoretically be split into "ss", so perhaps this should be normalized
    // out by a sophisticated tokenizer.
    public static final String GERMAN = Normalizer.normalize("Die Nationalmannschaft hat die Weltmeisterschaft gewonnen. Hörte ich während ich die Friedrichstraße hinunterlief.", Normalizer.Form.NFC);

    // (An attempt to) render the original Aristotelian syllogism in Greek.
    // It uses monotonic orthography, as is common in Modern Greek except
    // that Socrates' name is (in one instance) written with the old
    // orthography (and in capitals to look at how lower case handling
    // of a capital sigma at the end of a word is handled).
    public static final String GREEK = "Όλοι οι άνθρωποι είναι θνητοί. Ο ΣΩΚΡΆΤΗΣ είναι άνθρωπος. Ο Σωκρᾰ́της είναι θνητός.";

    // Translates to: "I do not speak Korean." It uses Hangul, which uses spaces as the
    // word separator token unlike Hanja (the Chinese-based system). It is also normalized
    // so that every character is its own codepoint, whereas the Hangul Jamo system combines
    // codepoints together.
    public static final String KOREAN = Normalizer.normalize("나는 한국어를 못해.", Normalizer.Form.NFC);

    // The Hebrew word for the Hebrew letter "nun" (makes the "n" sound).
    // The rendering of this letter includes a dagesh (dot), which maybe
    // should be filtered out. The final letter of the word (which happens to
    // be a nun) is in its "final" form, so it is position independent, so
    // it should arguably be normalized to the non-final form for searching
    // purposes.
    public static final String NUN = "נוּן";

    // Text containing the old letter "s". There are some stylistic ligatures that I'm pretty sure
    // at least used to exist (like "ſh" and "ſi") that I'd like to have in here but aren't for
    // whatever reason.
    public static final String OLD_S = "Engliſh uſed to have multiple verſions of the letter \"s\".";

    // Prologue to Shakespeare's Romeo and Juliet (taken from a public domain source).
    public static final String ROMEO_AND_JULIET_PROLOGUE = "" +
            "Two households, both alike in dignity,\n" +
            "In fair Verona, where we lay our scene,\n" +
            "From ancient grudge break to new mutiny,\n" +
            "Where civil blood makes civil hands unclean.\n" +
            "From forth the fatal loins of these two foes\n" +
            "A pair of star-cross’d lovers take their life;\n" +
            "Whose misadventur’d piteous overthrows\n" +
            "Doth with their death bury their parents’ strife.\n" +
            "The fearful passage of their death-mark’d love,\n" +
            "And the continuance of their parents’ rage,\n" +
            "Which, but their children’s end, nought could remove,\n" +
            "Is now the two hours’ traffic of our stage;\n" +
            "The which, if you with patient ears attend,\n" +
            "What here shall miss, our toil shall strive to mend.";

    // Translates to: "The actor visited many points or places of interest in Moscow."
    // Note the presence of the letter "ё" and the declension of "Москва" to the
    // genetive "Москвы". It also contains a 21 letter word, which isn't uncommon for
    // synthetic languages. The letter "ё" is sometimes rendered as "е". This snippet
    // also includes stress marks on the words, which isn't common among native
    // speakers, but sometimes happens (so it's possible that the correct thing to
    // do is to strip them out).
    public static final String RUSSIAN = "Актёр посет́ил мн́огие достопримеч́ательности Москв́ы.";

    // Translates to: "Or is it just nonsense?"
    // Telugu uses an abugida where vowels are encoded as combining diacritical marks.
    // Unlike many abjads (like Hebrew and Arabic), where vowel marks are usually omitted
    // by native speakers, native abugida users usually keep vowel markers, so tokenizers
    // should (arguably) not strip them away.
    public static final String TELUGU = "లేదా అది అర్ధంలేనిదేనా?";

    // Translates to: "Thai orthography is very complicated." (or that is the intention).
    // Whether or not the translation is true, the sentence is. Note the lack of
    // spaces between words.
    public static final String THAI = "การสะกดการันต์ไทยมีความซับซ้อนมาก";

    // A URL to see how that gets "tokenized".
    public static final String URL = "https://www.example.com/fake-path/1932e32ab3efc0014228eadc28219da2/hm";

    // Translates to: "A language is a dialect with an army and a navy." - Max Weinrich (1945)
    // Contains right-to-left text as well as usage of niqqud, which are usually included
    // in Yiddish, but not in Hebrew.
    public static final String YIDDISH = "אַ שפּראַך איז אַ דיאַלעקט מיט אַן אַרמיי און פֿלאָט.";

    public static final List<String> ALL = Arrays.asList(
            ANGSTROM,
            AETHELRED,
            BLACKBOARD,
            CHINESE_SIMPLIFIED,
            CHINESE_TRADITIONAL,
            EMOJIS,
            FIRE,
            FRENCH,
            GERMAN,
            GREEK,
            KOREAN,
            NUN,
            OLD_S,
            ROMEO_AND_JULIET_PROLOGUE,
            RUSSIAN,
            TELUGU,
            THAI,
            URL,
            YIDDISH
    );
}
