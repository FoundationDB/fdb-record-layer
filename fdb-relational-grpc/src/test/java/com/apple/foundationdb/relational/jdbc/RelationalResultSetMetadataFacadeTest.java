/*
 * RelationalStructFacadeTest.java
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

import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Stream;

public class RelationalResultSetMetadataFacadeTest {

    private static ResultSetMetadata createResultSetProtoForType(Type type) {
        return ResultSetMetadata.newBuilder()
                .setColumnMetadata(ListColumnMetadata.newBuilder()
                        .addColumnMetadata(ColumnMetadata.newBuilder()
                                .setName("foo")
                                .setType(type)
                                .build())
                        .build())
                .build();
    }

    static Stream<Arguments> protobufPrimitiveTypeProvider() {
        return Stream.of(
                Arguments.of(Type.INTEGER, Types.INTEGER),
                Arguments.of(Type.LONG, Types.BIGINT),
                Arguments.of(Type.STRING, Types.VARCHAR),
                Arguments.of(Type.ENUM, Types.OTHER),
                Arguments.of(Type.UUID, Types.OTHER),
                Arguments.of(Type.BOOLEAN, Types.BOOLEAN),
                Arguments.of(Type.BYTES, Types.BINARY),
                Arguments.of(Type.FLOAT, Types.FLOAT),
                Arguments.of(Type.DOUBLE, Types.DOUBLE),
                Arguments.of(Type.FLOAT, Types.FLOAT),
                Arguments.of(Type.UNKNOWN, null),
                Arguments.of(Type.VERSION, null),
                Arguments.of(Type.NULL, null)
        );
    }

    @ParameterizedTest
    @MethodSource("protobufPrimitiveTypeProvider")
    void testColumnType(Type type, Integer expectedSqlTypeCode) throws SQLException {
        final var metadata = new RelationalResultSetMetaDataFacade(createResultSetProtoForType(type));
        if (expectedSqlTypeCode == null) {
            Assertions.assertThrows(SQLException.class, () -> metadata.getColumnType(1));
        } else {
            final var actual = metadata.getColumnType(1);
            Assertions.assertEquals(expectedSqlTypeCode, actual);
        }
    }

    @Test
    void testStructMetadata() throws SQLException {
        final var metadata = new RelationalResultSetMetaDataFacade(ResultSetMetadata.newBuilder()
                .setColumnMetadata(ListColumnMetadata.newBuilder()
                        .addColumnMetadata(ColumnMetadata.newBuilder()
                                .setName("foo")
                                .setType(Type.STRUCT)
                                .setStructMetadata(ListColumnMetadata.newBuilder()
                                        .addColumnMetadata(ColumnMetadata.newBuilder().setType(Type.INTEGER).setName("bar"))
                                        .addColumnMetadata(ColumnMetadata.newBuilder().setType(Type.INTEGER).setName("baz"))
                                )
                        )
                ).build());
        Assertions.assertEquals(metadata.getColumnType(1), Types.STRUCT);
        final var actualStructMetadata = Assertions.assertInstanceOf(RelationalStructMetaData.class, metadata.getStructMetaData(1));
        Assertions.assertEquals(actualStructMetadata.getColumnType(1), Types.INTEGER);
        Assertions.assertEquals(actualStructMetadata.getColumnType(2), Types.INTEGER);
    }

    @Test
    void testArrayMetadata() throws SQLException {
        final var metadata = new RelationalResultSetMetaDataFacade(ResultSetMetadata.newBuilder()
                .setColumnMetadata(ListColumnMetadata.newBuilder()
                        .addColumnMetadata(ColumnMetadata.newBuilder()
                                .setName("foo")
                                .setType(Type.ARRAY)
                                .setArrayMetadata(ColumnMetadata.newBuilder().setType(Type.INTEGER).setName("bar"))
                        )
                ).build());
        Assertions.assertEquals(metadata.getColumnType(1), Types.ARRAY);
        // We do not yet implement getArrayMetaData yet, probably because the JDBC support for arrays is not
        // tested rigorously.
        Assertions.assertThrows(SQLException.class, () -> metadata.getArrayMetaData(1));
    }
}
