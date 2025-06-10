/*
 * ParserTests.java
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

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.RowStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.BitSet;

public class QueryParserTests {

    @Nonnull
    private static final RecordLayerSchemaTemplate fakeSchemaTemplate = RecordLayerSchemaTemplate
            .newBuilder()
            .setName("testTemplate")
            .addTable(RecordLayerTable
                    .newBuilder(false)
                    .setName("testTable")
                    .addColumn(RecordLayerColumn
                            .newBuilder()
                            .setName("testColumn")
                            .setDataType(DataType.Primitives.BOOLEAN.type())
                            .build())
                    .build())
            .build();

    private static final BitSet emptyBitSet = new BitSet();

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

    @Nonnull
    private static RowStruct createStruct() {
        final var metadata = new RelationalStructMetaData(
                FieldDescription.primitive("fInt", Types.INTEGER, DatabaseMetaData.columnNullable),
                FieldDescription.primitive("fLong", Types.BIGINT, DatabaseMetaData.columnNullable)
        );
        final var row = new ArrayRow(null, 1L);
        return new ImmutableRowStruct(row, metadata);
    }
}
