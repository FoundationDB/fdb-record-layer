/*
 * SqlCommentsParserTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parser-level tests for SQL comment handling. They assert that comments never reach the parse tree, i.e. a query with
 * comments produces the exact same tree as the equivalent comment-free query. End-to-end behaviour (results, plan-cache
 * equivalence) is covered by {@code sql-comments.yamsql}.
 * <p>
 * Only two comment styles are supported: ANSI/PostgreSQL line comments ({@code --}) and C-style block comments
 * ({@code /*}&hellip;{@code *}{@code /}). MySQL-specific styles ({@code #} line comments and {@code /*!} executable
 * comments) are intentionally not supported. Because comments are handled purely at the lexical level (routed to a
 * hidden channel), they carry no semantics; hint-style comments would require parser-level support and are out of
 * scope.
 */
public class SqlCommentsParserTests {

    /**
     * Asserts that {@code queryWithComments} parses to the same parse tree as {@code equivalentQuery}, proving the
     * comments were routed to a hidden channel and stripped before reaching the tree.
     */
    private static void assertSameTree(@Nonnull final String queryWithComments,
                                       @Nonnull final String equivalentQuery) throws RelationalException {
        final var withComments = QueryParser.parse(queryWithComments).getRootContext().getText();
        final var withoutComments = QueryParser.parse(equivalentQuery).getRootContext().getText();
        assertThat(withComments).isEqualTo(withoutComments);
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
        //
        // ANSI/PostgreSQL behavior '--' starts a comment even when not followed by whitespace.
        //
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
        //
        // 'SELECT 1--1' -> the '--1' (and the rest of the line) is a comment, so this equals 'SELECT 1 FROM T'.
        //
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
        //
        // The 'root' rule accepts zero statements, so a query that is entirely a comment parses cleanly
        // (as an empty statement) rather than raising a parse error.
        //
        final var text = QueryParser.parse("-- just a comment").getRootContext().getText();
        assertThat(text).isEqualTo(QueryParser.parse("").getRootContext().getText());
    }

    @Test
    void unterminatedBlockCommentIsSyntaxError() {
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT A FROM T /* unterminated"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void hashIsNotALineComment() {
        //
        // MySQL-style '#' line comments are intentionally unsupported: '#' is not a valid token, so a query using
        // it as a comment is a syntax error rather than being silently stripped.
        //
        RelationalAssertions.assertThrows(() -> QueryParser.parse("SELECT A FROM T WHERE A > 0 # hash comment"))
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void mysqlExecutableCommentIsTreatedAsOrdinaryBlockComment() throws Exception {
        //
        // MySQL '/*! ... */' executable comments carry no special meaning here; they fall through to the ordinary
        // block-comment rule and are stripped just like any other comment.
        //
        assertSameTree("SELECT A FROM T /*! WHERE A > 0 */",
                "SELECT A FROM T");
    }
}
