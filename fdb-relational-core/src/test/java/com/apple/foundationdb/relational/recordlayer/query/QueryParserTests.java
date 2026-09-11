/*
 * ParserTests.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryParserTests {

    @ParameterizedTest
    @ValueSource(strings = {"__foo", "2foo", "#foo", ".foo", "__"})
    void invalidIdentifierTest(String id) {
        final var query = "SELECT * FROM " + id;

        // attempting to parse unquoted invalid identifiers should throw a syntax error.
        RelationalAssertions.assertThrows(() -> QueryParser.parse(query))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);

        // ... same errors should be thrown even if the identifiers are quoted.
        final var queryWithQuotes = "SELECT * FROM '" + id + "'";
        RelationalAssertions.assertThrows(() -> QueryParser.parse(query))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    /**
     * Tests the forms of {@code ARRAY_AGG()} that the grammar admits.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "ARRAY_AGG(val)",
            "ARRAY_AGG(ALL val)",
            "ARRAY_AGG(DISTINCT val)",
            "ARRAY_AGG(val IGNORE NULLS)",
            "ARRAY_AGG(val RESPECT NULLS)",
            "ARRAY_AGG(val ORDER BY val)",
            "ARRAY_AGG(val ORDER BY val DESC NULLS FIRST)",
            "ARRAY_AGG(val IGNORE NULLS ORDER BY val)",
            "ARRAY_AGG(DISTINCT val IGNORE NULLS ORDER BY val DESC, id)",
            "ARRAY_AGG(val) OVER ()",
            "ARRAY_AGG(val IGNORE NULLS) OVER (PARTITION BY grp)",
    })
    void arrayAggParsesTest(String functionCall) throws RelationalException {
        QueryParser.parse("SELECT " + functionCall + " FROM T1");
    }

    /**
     * The null treatment clause precedes the in-call {@code ORDER BY} clause. The opposite order does not parse.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "ARRAY_AGG(val ORDER BY val IGNORE NULLS)",
            "ARRAY_AGG(val ORDER BY val RESPECT NULLS)",
            "ARRAY_AGG(IGNORE NULLS)",
            "ARRAY_AGG(val IGNORE)",
            "ARRAY_AGG(val NULLS)",
            "SUM(val IGNORE NULLS)",
            "COUNT(val IGNORE NULLS)",
            "GROUP_CONCAT(val IGNORE NULLS)",
    })
    void arrayAggDoesNotParseTest(String functionCall) {
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT " + functionCall + " FROM T1"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    /**
     * The {@code ARRAY_AGG} and {@code RESPECT} tokens are added to {@code keywordsCanBeId}, so they remain usable as
     * identifiers.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT array_agg FROM T1",
            "SELECT respect FROM T1",
            "SELECT val AS array_agg FROM T1",
            "SELECT val AS respect FROM T1",
            "SELECT array_agg.val FROM T1 AS array_agg",
            "SELECT respect.val FROM T1 AS respect",
            "SELECT * FROM array_agg",
            "SELECT * FROM respect",
            "INSERT INTO array_agg (respect) VALUES (42)",
            "CREATE SCHEMA TEMPLATE ts CREATE TABLE array_agg (respect BIGINT, PRIMARY KEY (respect))",
    })
    void newKeywordsRemainUsableAsIdentifiersTest(String query) throws RelationalException {
        QueryParser.parse(query);
    }

    /**
     * Asserts that {@code queryWithComments} parses to the same parse tree as {@code equivalentQuery}, proving the
     * comments were skipped by the lexer and never reached the tree.
     */
    private static void assertSameTree(@Nonnull final String queryWithComments,
                                       @Nonnull final String equivalentQuery) throws RelationalException {
        final var withComments = QueryParser.parse(queryWithComments).getRootContext().getText();
        final var withoutComments = QueryParser.parse(equivalentQuery).getRootContext().getText();
        assertThat(withComments).isEqualTo(withoutComments);
    }

    /**
     * Asserts that {@code query} parses and that {@code expectedFragment} survives into the parse tree verbatim.
     * Used for text that looks like a comment but is not one, where the point is that nothing was stripped rather
     * than that two queries agree.
     */
    private static void assertTreeContains(@Nonnull final String query,
                                           @Nonnull final String expectedFragment) throws RelationalException {
        assertThat(QueryParser.parse(query).getRootContext().getText()).contains(expectedFragment);
    }

    @Test
    void blockCommentIsStripped() throws Exception {
        assertSameTree("SELECT A /* pick column A */ FROM T WHERE A > 0",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void multiLineBlockCommentIsStripped() throws Exception {
        assertSameTree("SELECT A\n/* this comment\n   spans multiple\n   lines */\nFROM T WHERE A > 0",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void lineCommentWithSpaceIsStripped() throws Exception {
        assertSameTree("SELECT A FROM T WHERE A > 0 -- trailing comment",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void lineCommentWithoutSpaceIsStripped() throws Exception {
        // ANSI/PostgreSQL behavior '--' starts a comment even when not followed by whitespace.
        assertSameTree("SELECT A FROM T WHERE A > 0 --no space after the dashes",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void lineCommentOnItsOwnLineIsStripped() throws Exception {
        assertSameTree("SELECT A\n-- choose the rows\nFROM T\nWHERE A > 0",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void everythingAfterLineCommentIsIgnored() throws Exception {
        // The 'AND A < 100' is part of the comment and must not affect the tree.
        assertSameTree("SELECT A FROM T WHERE A > 0 -- AND A < 100",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void commentTextThatLooksLikeSqlIsIgnored() throws Exception {
        assertSameTree("SELECT A FROM T /* SELECT * FROM OTHER; DROP TABLE T */ WHERE A > 0",
                "SELECT A FROM T WHERE A > 0");
    }

    @Test
    void doubleDashStartsCommentWithoutSpace() throws Exception {
        // 'SELECT 1--1' -> the '--1' (and the rest of the line) is a comment, so this equals 'SELECT 1 FROM T'.
        assertSameTree("SELECT 1--1\nFROM T",
                "SELECT 1 FROM T");
    }

    @Test
    void spacedDoubleMinusRemainsArithmetic() throws Exception {
        //
        // With spaces, '- -' is subtraction of a negative literal, NOT a comment. It must differ from the
        // commented-out form, guarding the intended dialect choice against a future grammar regression.
        //
        final var arithmetic = QueryParser.parse("SELECT 3 - -2 FROM T").getRootContext().getText();
        final var commented = QueryParser.parse("SELECT 3 FROM T").getRootContext().getText();
        assertThat(arithmetic).isNotEqualTo(commented);
    }

    @Test
    void commentOnlyQueryParsesAsEmptyStatement() throws Exception {
        // The 'root' rule accepts zero statements, so a query that is entirely a comment parses cleanly
        // (as an empty statement) rather than raising a parse error.
        final var text = QueryParser.parse("-- just a comment").getRootContext().getText();
        assertThat(text).isEqualTo(QueryParser.parse("").getRootContext().getText());
    }

    @Test
    void unterminatedBlockCommentIsSyntaxError() {
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT A FROM T /* unterminated"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void lineCommentIsTerminatedByEveryLineEndingStyle() throws Exception {
        // LF, CRLF, and a bare CR all end a line comment, and so does the end of the input. The bare CR is the
        // interesting one: PostgreSQL treats a lone carriage return as a line ending rather than merely the first
        // half of a CRLF. Verified against PostgreSQL 18.4, where 'SELECT 1 --c\r + 2' evaluates to 3.
        assertSameTree("SELECT A FROM T -- c\nWHERE A > 0", "SELECT A FROM T WHERE A > 0");
        assertSameTree("SELECT A FROM T -- c\r\nWHERE A > 0", "SELECT A FROM T WHERE A > 0");
        assertSameTree("SELECT A FROM T -- c\rWHERE A > 0", "SELECT A FROM T WHERE A > 0");
        assertSameTree("SELECT A FROM T -- c", "SELECT A FROM T");
    }

    @Test
    void blockCommentsNest() throws Exception {
        // A '/*' inside a block comment opens an inner comment that has to be closed before the outer one, so
        // commenting out a region that already contains a comment removes the whole region. Verified against
        // PostgreSQL 18.4, which nests the same way.
        assertSameTree("SELECT A /* outer /* inner */ still outer */ FROM T",
                "SELECT A FROM T");
        assertSameTree("SELECT A /* a /* b /* c */ d */ e */ FROM T",
                "SELECT A FROM T");
    }

    @Test
    void nestedBlockCommentLeftOpenIsSyntaxError() {
        //
        // The inner '*/' closes only the inner comment, so the outer one is still open at the end of the input.
        // This is the case that makes nesting worth implementing: without it the comment would end at the first
        // '*/' and the trailing 'WHERE A > 0' would silently become part of the query again.
        //
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT A FROM T /* outer /* inner */ WHERE A > 0"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void blockCommentMarkerInsideStringLiteralDoesNotOpenAComment() throws Exception {
        // A '/*' inside a string literal is data. It must not open a comment, which would otherwise swallow the
        // rest of the statement and, with nesting, leave it unterminated.
        assertTreeContains("SELECT A /* c */ FROM T WHERE B = 'a /* b'", "'a /* b'");
    }

    @Test
    void commentMarkersInsideQuotedIdentifierAreNotStripped() throws Exception {
        // A quoted identifier is lexed as one token, so comment markers inside it are part of the name rather than
        // the start of a comment. PostgreSQL 18.4 agrees: '"a--b"' is stored as the four characters a, -, -, b.
        assertTreeContains("SELECT A AS \"a--b\" FROM T", "\"a--b\"");
        assertTreeContains("SELECT A AS \"a/*b*/c\" FROM T", "\"a/*b*/c\"");
    }

    @Test
    void multiLineQuotedIdentifierIsNotTruncatedByLineComment() throws Exception {
        // A quoted identifier may span lines, and a '--' on its second line is still part of the name: the line
        // comment rule never gets to run inside the identifier token. The bare CR case matters here too, now that a
        // CR ends a line comment.
        assertTreeContains("SELECT A AS \"a\n-- b\" FROM T", "\"a\n-- b\"");
        assertTreeContains("SELECT A AS \"a\r-- b\" FROM T", "\"a\r-- b\"");
    }

    @Test
    void hashIsNotALineComment() {
        // MySQL-style '#' line comments are intentionally unsupported: '#' is not a valid token, so a query using
        // it as a comment is a syntax error rather than being silently stripped.
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT A FROM T WHERE A > 0 # hash comment"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void mysqlExecutableCommentIsTreatedAsOrdinaryBlockComment() throws Exception {
        // MySQL '/*! ... */' executable comments carry no special meaning here; they fall through to the ordinary
        // block-comment rule and are stripped just like any other comment.
        assertSameTree("SELECT A FROM T /*! WHERE A > 0 */",
                "SELECT A FROM T");
    }
}
