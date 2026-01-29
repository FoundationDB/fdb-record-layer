/*
 * QueryTypeTests.java
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

import com.apple.foundationdb.relational.api.ParseTreeInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * Verifies that {@link QueryParser#parse(String)} returns the correct query type.
 */
public class QueryTypeTests {

    static class QueriesProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of("select count(*) from restaurant", ParseTreeInfo.QueryType.SELECT),
                    Arguments.of("  select * from restaurant", ParseTreeInfo.QueryType.SELECT),
                    Arguments.of("  SelECT * from restaurant", ParseTreeInfo.QueryType.SELECT),
                    Arguments.of("create schema template st1 create table A(A1 bigint, A2 bigint, A3 bigint, primary key(A1))", ParseTreeInfo.QueryType.CREATE),
                    Arguments.of("  create schema template st1 create table A(A1 bigint, A2 bigint, A3 bigint, primary key(A1))", ParseTreeInfo.QueryType.CREATE),
                    Arguments.of("CrEaTe schema template st1 create table A(A1 bigint, A2 bigint, A3 bigint, primary key(A1))", ParseTreeInfo.QueryType.CREATE),
                    Arguments.of("insert into t1 values (42)", ParseTreeInfo.QueryType.INSERT),
                    Arguments.of("  iNsErT into t1 values (42)", ParseTreeInfo.QueryType.INSERT),
                    Arguments.of("insert into t1 select * from t1", ParseTreeInfo.QueryType.INSERT),
                    Arguments.of("update t1 set col1 = 42", ParseTreeInfo.QueryType.UPDATE),
                    Arguments.of("UpDate t1 set col1 = col + 1", ParseTreeInfo.QueryType.UPDATE),
                    Arguments.of(" UpDate t1 set col1 = col + 1", ParseTreeInfo.QueryType.UPDATE),
                    Arguments.of("DELETE from T1", ParseTreeInfo.QueryType.DELETE),
                    Arguments.of("  delete from T1", ParseTreeInfo.QueryType.DELETE),
                    Arguments.of("  dEleTe from T1", ParseTreeInfo.QueryType.DELETE)
            );
        }
    }

    @ParameterizedTest(name = "{0} IS {1}")
    @ArgumentsSource(QueriesProvider.class)
    public void queryTypeIsSetCorrectly(@Nonnull final String query, @Nonnull ParseTreeInfo.QueryType expectedType) throws Exception {
        final var parseInfo = QueryParser.parse(query);
        Assertions.assertEquals(expectedType, parseInfo.getQueryType());
    }

    @Test
    public void createBitmapIndexParsesCorrectly() throws Exception {
        var parseInfo = QueryParser.parse("CREATE SCHEMA TEMPLATE blahblah " +
                "CREATE INDEX all_seen_uids_bitmap AS SELECT bitmap_construct_agg(bitmap_bit_position(uid)) FROM msgstate GROUP BY mboxRef, isSeen");
        Assertions.assertEquals(ParseTreeInfo.QueryType.CREATE, parseInfo.getQueryType());
        parseInfo = QueryParser.parse("SELECT bitmap_construct_agg(bitmap_bit_position(uid)) FROM msgstate WHERE mboxRef=\"INBOX\" AND isSeen=0");
        Assertions.assertEquals(ParseTreeInfo.QueryType.SELECT, parseInfo.getQueryType());
    }
}
