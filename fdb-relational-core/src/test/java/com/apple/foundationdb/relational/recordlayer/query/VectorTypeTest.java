/*
 * VectorTypeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.StructResultSetMetaData;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.stream.Stream;

public class VectorTypeTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();


    @Nonnull
    public static Stream<Arguments> vectorArguments() {
        return Stream.of(
                Arguments.of("halfvector(512)", DataType.VectorType.of(16, 512, true)),
                Arguments.of("vector16(512)", DataType.VectorType.of(16, 512, true)),
                Arguments.of("doublevector(1024)", DataType.VectorType.of(64, 1024, true)),
                Arguments.of("vector32(768)", DataType.VectorType.of(32, 768, true)),
                Arguments.of("vector(256)", DataType.VectorType.of(16, 256, true)));
    }

    @ParameterizedTest(name = "{0} evaluates to data type {1}")
    @MethodSource("vectorArguments")
    void vectorTest(@Nonnull final String ddlType, @Nonnull final DataType expectedType) throws Exception {
        final String schemaTemplate = "create table t1(id bigint, col1 " + ddlType + ", primary key(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("select * from t1");
                final var metadata = statement.getResultSet().getMetaData();
                Assertions.assertThat(metadata).isInstanceOf(StructResultSetMetaData.class);
                final var relationalMetadata = (StructResultSetMetaData)metadata;
                final var type = relationalMetadata.getRelationalDataType().getFields().get(1).getType();
                Assertions.assertThat(type).isEqualTo(expectedType);
            }
        }
    }
}
