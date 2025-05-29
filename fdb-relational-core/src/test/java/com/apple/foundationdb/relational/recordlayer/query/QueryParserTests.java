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

import com.apple.foundationdb.record.PlanHashable;
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.BitSet;
import java.util.TreeMap;
import java.util.stream.Stream;

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

    private static final class PreparedParametersSupplier implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            return Stream.of(
                    Arguments.of(0, "0"),
                    Arguments.of(42L, "42"),
                    Arguments.of(42, "42"),
                    Arguments.of(3.14, "3.14"),
                    Arguments.of(true, "true"),
                    Arguments.of(false, "false"),
                    Arguments.of(null, "<<NULL>>"),
                    Arguments.of("foo", "'foo'"),
                    Arguments.of( new Object[]{10L, 11L}, "Array[10,11]"),
                    Arguments.of(new int[]{1, 2}, "Array[1,2]"),
                    Arguments.of(new byte[] {0xA, 0x1, 0x2}, "Array[0a,01,02]"),
                    Arguments.of( new Object[]{new Object[] {"foo", "bar"}, new Object[]{10L, 11L}}, "Array[Array['foo','bar'],Array[10,11]]"),
                    Arguments.of( new Object[]{new Object[] {"foo", "bar"}, new Object[]{10L, 11L}}, "Array[Array['foo','bar'],Array[10,11]]"),
                    Arguments.of(createStruct(), "Struct{<<NULL>>,1}")
            );
        }
    }

    @ParameterizedTest(name = "replacing prepared parameter {0}")
    @ArgumentsSource(PreparedParametersSupplier.class)
    void replacePreparedParams(Object preparedParameter, String expectedParamReplacement) throws Exception {
        final var query = "create temporary function sq1 ( in x bigint ) on commit drop function as select * from t1 where col1 = ";
        final var expectedQuery = query + expectedParamReplacement + " ";

        // try unnamed params
        var preparedQuery = query + "?";
        final var unnamedParameters = new TreeMap<Integer, Object>();
        unnamedParameters.put(1, preparedParameter);
        var preparedParams = PreparedParams.ofUnnamed(unnamedParameters);
        var result = processQuery(preparedQuery, preparedParams);
        Assertions.assertThat(result).isEqualTo(expectedQuery);

        // try named params
        preparedQuery = query + "?foo";
        final var namedParameters = new TreeMap<String, Object>();
        namedParameters.put("foo", preparedParameter);
        preparedParams = PreparedParams.ofNamed(namedParameters);
        result = processQuery(preparedQuery, preparedParams);
        Assertions.assertThat(result).isEqualTo(expectedQuery);
    }

    @Nonnull
    private static String processQuery(@Nonnull final String preparedQuery, @Nonnull final PreparedParams preparedParams) throws Exception {
        final var normalized = AstNormalizer.normalizeAst(fakeSchemaTemplate, QueryParser.parse(preparedQuery).getRootContext(),
                PreparedParams.copyOf(preparedParams), 0, emptyBitSet, false, PlanHashable.PlanHashMode.VC0,
                preparedQuery);
        return QueryParser.replacePreparedParams(normalized.getParseTree(), preparedParams);
    }
}
