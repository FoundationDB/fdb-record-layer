/*
 * ResultSetContinuationTest.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ResultSetContinuationTest {

    public static Stream<Arguments> continuationSource() {
        return Stream.of(
                Arguments.of(MockContinuation.BEGIN),
                Arguments.of(MockContinuation.END),
                Arguments.of(new MockContinuation(Continuation.Reason.TRANSACTION_LIMIT_REACHED, new byte[]{1}, false, false)),
                Arguments.of(new MockContinuation(Continuation.Reason.CURSOR_AFTER_LAST, new byte[]{1}, false, true)),
                Arguments.of(new MockContinuation(Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED, new byte[0], false, true)));
    }

    @ParameterizedTest
    @MethodSource("continuationSource")
    void testResultSetWithContinuation(Continuation continuation) throws Exception {
        RelationalResultSet resultSet = TestUtils.resultSet(
                "TestType",
                List.of(Types.INTEGER, Types.INTEGER, Types.INTEGER),
                continuation,
                TestUtils.row(1, 2, 3), TestUtils.row(4, 5, 6));
        ResultSet rsProto = TypeConversion.toProtobuf(resultSet);

        try (RelationalResultSetFacade deserializedResultSet = new RelationalResultSetFacade(rsProto)) {
            List<List<Integer>> rows = toRows(deserializedResultSet, 3);
            Assertions.assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6)), rows);
            Continuation convertedContinuation = deserializedResultSet.getContinuation();
            assertContinuation(continuation, convertedContinuation);
        }
    }

    @ParameterizedTest
    @MethodSource("continuationSource")
    void testContinuationRoundTrip(Continuation continuation) throws Exception {
        RelationalResultSet resultSet = TestUtils.resultSet(
                "TestType",
                List.of(Types.INTEGER, Types.INTEGER, Types.INTEGER),
                continuation,
                TestUtils.row(1, 2, 3), TestUtils.row(4, 5, 6));
        ResultSet rsProto = TypeConversion.toProtobuf(resultSet);

        try (RelationalResultSetFacade deserializedResultSet = new RelationalResultSetFacade(rsProto)) {
            byte[] returnedContinuation = resultSet.getContinuation().serialize();
            byte[] expectedContinuation = continuation.serialize();
            Assertions.assertArrayEquals(expectedContinuation, returnedContinuation);
        }
    }

    @Test
    void testResultSetWithNullContinuationFails() throws Exception {
        RelationalResultSet resultSet = TestUtils.resultSet(
                "TestType",
                List.of(Types.INTEGER, Types.INTEGER, Types.INTEGER),
                null,
                TestUtils.row(1, 2, 3), TestUtils.row(4, 5, 6));
        Assertions.assertThrows(NullPointerException.class, () -> TypeConversion.toProtobuf(resultSet));
    }

    private void assertContinuation(Continuation expected, Continuation actual) {
        Assertions.assertEquals(expected.atBeginning(), actual.atBeginning());
        Assertions.assertEquals(expected.atEnd(), actual.atEnd());
        Assertions.assertEquals(expected.getReason(), actual.getReason());
        // This, again, assumes that the entire inner continuation is serialized as the state
        Assertions.assertArrayEquals(expected.serialize(), actual.getExecutionState());
    }

    private List<List<Integer>> toRows(RelationalResultSetFacade convertedResultSet, int numColumns) throws SQLException {
        List<List<Integer>> result = new ArrayList<>();
        while (convertedResultSet.next()) {
            result.add(toRow(convertedResultSet, numColumns));
        }
        return result;
    }

    private List<Integer> toRow(java.sql.ResultSet resultSet, int numColumns) throws SQLException {
        List<Integer> result = new ArrayList<>();
        for (int i = 1; i <= numColumns; i++) {
            result.add(resultSet.getInt(i));
        }
        return result;
    }
}
