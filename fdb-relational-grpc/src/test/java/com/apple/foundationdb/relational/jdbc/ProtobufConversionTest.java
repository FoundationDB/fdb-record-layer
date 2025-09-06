/*
 * ProtobufConversionTest.java
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
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuation;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuationReason;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Struct;
import com.apple.foundationdb.relational.utils.OptionsTestHelper;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ProtobufConversionTest {
    @Test
    public void testToColumnStringBiFunction() {
        BiFunction<String, Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearString() : b.setString(a);
        Column column = TypeConversion.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasString());
        String a = "abc";
        column = TypeConversion.toColumn(a, biFunction);
        Assertions.assertEquals(a, column.getString());
    }

    @Test
    public void testToColumnBinaryBiFunction() {
        BiFunction<byte[], Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearBinary() : b.setBinary(ByteString.copyFrom(a));
        Column column = TypeConversion.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasBinary());
        byte[] a = new byte[]{'a', 'b', 'c'};
        column = TypeConversion.toColumn(a, biFunction);
        for (int i = 0; i < a.length; i++) {
            Assertions.assertEquals(a[i], column.getBinary().toByteArray()[i]);
        }
    }

    @Test
    public void testToColumnIntegerBiFunction() {
        BiFunction<Integer, Column.Builder, Column.Builder> biFunction =
                (a, b) -> a == null ? b.clearInteger() : b.setInteger(a);
        Column column = TypeConversion.toColumn(null, biFunction);
        Assertions.assertFalse(column.hasInteger());
        Integer a = 123;
        column = TypeConversion.toColumn(a, biFunction);
        Assertions.assertEquals(a, column.getInteger());
    }

    @Test
    void testOptions() throws Exception {
        final Options nonDefault = OptionsTestHelper.nonDefaultOptions();
        final com.apple.foundationdb.relational.jdbc.grpc.v1.Options asProto = TypeConversion.toProtobuf(nonDefault).build();
        final Options fromProto = TypeConversion.fromProtobuf(asProto);
        Assertions.assertEquals(nonDefault, fromProto);
    }

    @Test
    void testResultSetWithContinuation() throws Exception {
        MockContinuation continuation = new MockContinuation(Continuation.Reason.TRANSACTION_LIMIT_REACHED, null, false, false);
        RelationalResultSet resultSet = TestUtils.resultSet(
                continuation,
                TestUtils.row(1, 2, 3), TestUtils.row(4, 5, 6));
        ResultSet converted = TypeConversion.toProtobuf(resultSet);

        Assertions.assertEquals(2, converted.getRowCount());
        assertRow(List.of(1, 2, 3), converted.getRow(0));
        assertRow(List.of(4, 5, 6), converted.getRow(1));
        assertContinuation(continuation.serialize(), RpcContinuationReason.TRANSACTION_LIMIT_REACHED, converted.getContinuation());
    }

    private void assertContinuation(byte[] state, RpcContinuationReason reason, RpcContinuation actual) {
        Assertions.assertArrayEquals(state, actual.getInternalState().toByteArray());
        Assertions.assertEquals(reason, actual.getReason());
    }

    private void assertRow(List<Integer> expected, Struct actual) {
        List<Integer> actualRow = actual.getColumns().getColumnList()
                .stream()
                .map(Column::getInteger)
                .collect(Collectors.toList());
        Assertions.assertEquals(expected, actualRow);
    }
}
