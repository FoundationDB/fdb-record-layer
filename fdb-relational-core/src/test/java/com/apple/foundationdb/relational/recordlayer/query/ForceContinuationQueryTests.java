/*
 * JoinWithLimitTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.recordlayer.UniqueIndexTests;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ForceContinuationQueryTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String getTemplate_definition =
            """
            create table t1(id bigint, col1 bigint, col2 bigint, primary key(id))
            create index mv1 as select count(*) from t1
            create index mv2 as select count(*) from t1 group by col2
            create index mv3 as select count(col1) from t1
            create index mv4 as select count(col1) from t1 group by col2

            create table t2(id bigint, col1 bigint, col2 bigint, col3 bigint, primary key(id))
            create index mv5 as select col2 from t2
            create index mv7 as select min_ever(col3) from t2

            create table t3(id bigint, col1 bigint, col2 bigint, primary key(id))
            create index t3_i1 as select count(*) from t3
            create index t3_i2 as select count(*) from t3 group by col1
            create index t3_i3 as select count(col2) from t3
            create index t3_i4 as select count(col2) from t3 group by col1
            create index t3_i5 as select sum(col1) from t3
            create index t3_i6 as select sum(col1) from t3 group by col2
            """;

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(UniqueIndexTests.class, getTemplate_definition);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(db::getConnectionUri)
            .withSchema(db.getSchemaName());

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public ForceContinuationQueryTests() {
    }

    @BeforeAll
    public static void beforeAll() {
        Utils.enableCascadesDebugger();
    }

    @BeforeEach
    void setup() throws Exception {
        statement.execute("INSERT INTO T1 VALUES (1, 10, 1), (2, null, 2), (3, null, 2), (4, 12, 2)");
        statement.execute("INSERT INTO T2 VALUES (1, 10, 1, 11), (2, null, 2, 20), (3, null, 2, 20), (4, 12, 2, 20)");
        statement.execute("insert into t3 values (1, 2, 3), (2, 2, 3), (3, 2, 3)");
        statement.execute("delete from t3");
    }

    // disabled until SerializationMode = TO_NEW
    @ParameterizedTest
    @MethodSource("failedQueries")
    void testNewSerialization(String sql, long result) throws Exception {
        Continuation continuation;
        try (final var s = connection.createStatement()) {
            s.setMaxRows(1);
            try (var resultSet = s.executeQuery(sql)) {
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(result, resultSet.getLong(1));
                continuation = resultSet.getContinuation();
            }
        }
        // new serialization fixed the issue
        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setMaxRows(1);
            preparedStatement.setBytes("param", continuation.serialize());
            try (var resultSet = preparedStatement.executeQuery()) {
                Assertions.assertFalse(resultSet.next());
            }
        }
    }

    private static Stream<Arguments> failedQueries() {
        return Stream.of(
                // aggregate-index-count.yamsql
                Arguments.of("select count(*) from t1", 4L),
                Arguments.of("select count(col1) from t1", 2L),
                // aggregate-index-tests.yamsql
                Arguments.of("select min_ever(col3) from t2", 11L),
                // aggregate-empty-table.yamsql
                Arguments.of("select count(*) from t3", 0L),
                Arguments.of("select count(col2) from t3", 0L),
                Arguments.of("select sum(col1) from t3", 0L)
        );
    }
}
