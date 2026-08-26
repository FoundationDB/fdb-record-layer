/*
 * StringLiteralNormalizationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.generated.RelationalLexer;
import com.apple.foundationdb.relational.generated.RelationalParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * Tests for the decoding of string literals, which the lexer defines: {@code SQUOTA_STRING} admits a
 * doubled quote as one unit inside a single-quoted string, denoting a single quote character.
 *
 * <p>The cases are supplied as {@link Arguments} rather than through {@code @CsvSource} on purpose —
 * the CSV parser uses the single quote as its own quote character and would strip the delimiters off
 * every literal here before the test ever saw them.
 */
public class StringLiteralNormalizationTest {

    @SuppressWarnings("PMD.CloseResource") // the ANTLR streams hold no OS resources
    private static RelationalParser.StringLiteralContext parseStringLiteral(final String literal) {
        final var lexer = new RelationalLexer(CharStreams.fromString(literal));
        final var parser = new RelationalParser(new CommonTokenStream(lexer));
        return parser.stringLiteral();
    }

    static Stream<Arguments> literals() {
        return Stream.of(
                // Controls. An implementation that only strips the outer delimiters already answers
                // these correctly, which is precisely why they cannot detect the defect.
                Arguments.of("'abc'", "abc"),
                Arguments.of("''", ""),
                // The escape denotes one quote character.
                Arguments.of("'it''s'", "it's"),
                Arguments.of("''''", "'"),
                Arguments.of("'a''''b'", "a''b"),
                Arguments.of("'''abc'", "'abc"),
                Arguments.of("'abc'''", "abc'")
        );
    }

    /**
     * Before the escape was decoded, {@code 'it''s'} evaluated to the five characters {@code it''s},
     * so the value carried the escape rather than the character it denotes.
     */
    @ParameterizedTest(name = "{0} decodes to {1}")
    @MethodSource("literals")
    void decodesTheDoubledQuoteEscape(final String literal, final String expected) {
        Assertions.assertEquals(expected, SemanticAnalyzer.normalizeStringLiteral(parseStringLiteral(literal)));
    }

    /**
     * Adjacent literals are separate tokens and concatenate.
     *
     * <p>This is why the decoding works from the TOKENS rather than from {@code getText()}: ANTLR
     * drops the original spacing, so {@code 'a' 'b'} and {@code 'a''b'} render identically and cannot
     * be told apart once concatenated. Decoding each token and then joining is the only reading that
     * is correct for both.
     */
    @Test
    void adjacentLiteralsConcatenateAndAreNotConfusedWithAnEscape() {
        Assertions.assertEquals("ab", SemanticAnalyzer.normalizeStringLiteral(parseStringLiteral("'a' 'b'")));
        Assertions.assertEquals("a'b", SemanticAnalyzer.normalizeStringLiteral(parseStringLiteral("'a''b'")));

        // The two inputs above share one text once ANTLR concatenates their tokens. Asserting that
        // here means a future rewrite which goes back to getText() cannot pass this class.
        Assertions.assertEquals(parseStringLiteral("'a' 'b'").getText(), parseStringLiteral("'a''b'").getText());
    }

    static Stream<Arguments> roundTrips() {
        return Stream.of(
                Arguments.of(""),
                Arguments.of("a"),
                Arguments.of("it's"),
                Arguments.of("'"),
                Arguments.of("''"),
                Arguments.of("'leading"),
                Arguments.of("trailing'"),
                Arguments.of("a'b'c"),
                Arguments.of("no quotes here"),
                Arguments.of("tab\tand newline\n"),
                Arguments.of("unicode äöü"),
                Arguments.of("back\\slash"),
                Arguments.of("\"double quotes\"")
        );
    }

    /**
     * Round-trips an arbitrary string through the grammar's own encoding and back.
     *
     * <p>This is the check that the decoding agrees with ANTLR rather than with a reading of the
     * lexer rule. {@code SQUOTA_STRING} is
     * {@code '\'' ('\'\'' | ~('\''))* '\''}, so the encoding of a string is: wrap in quotes, double
     * every interior quote. The test applies that encoding, hands the result to the REAL generated
     * lexer, decodes, and requires the original back. If the decoder and the lexer disagree about
     * what the escape means, the round trip cannot hold.
     *
     * <p>It also asserts the lexer produced exactly ONE {@code STRING_LITERAL} token. Without that,
     * a case where the lexer split the input into several tokens could still round-trip through the
     * concatenating decoder and hide the disagreement.
     */
    @ParameterizedTest(name = "round trip {0}")
    @MethodSource("roundTrips")
    void decodingInvertsTheGrammarsEncoding(final String raw) {
        final var literal = "'" + raw.replace("'", "''") + "'";
        final var ctx = parseStringLiteral(literal);
        Assertions.assertEquals(1, ctx.STRING_LITERAL().size(),
                "lexer did not read " + literal + " as a single STRING_LITERAL token");
        Assertions.assertEquals(raw, SemanticAnalyzer.normalizeStringLiteral(ctx));
    }

    /**
     * A charset-decorated literal keeps its previous handling. Those are rejected upstream of the
     * value path, and this decoding must not start producing a value for them.
     *
     * <p>The charset name is upper case because the lexer spells it that way
     * ({@code UTF8: 'UTF8';}); a lower-case {@code _utf8} does not lex as a charset name at all.
     */
    @Test
    void decoratedLiteralsAreLeftToTheLegacyNormalization() {
        final var ctx = parseStringLiteral("_UTF8'a'");
        Assertions.assertNotNull(ctx.STRING_CHARSET_NAME(), "fixture no longer parses as a decorated literal");
        Assertions.assertEquals(SemanticAnalyzer.normalizeString(ctx.getText(), false),
                SemanticAnalyzer.normalizeStringLiteral(ctx));
    }
}
