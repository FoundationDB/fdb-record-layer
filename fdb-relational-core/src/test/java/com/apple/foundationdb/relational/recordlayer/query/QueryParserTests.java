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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
}
