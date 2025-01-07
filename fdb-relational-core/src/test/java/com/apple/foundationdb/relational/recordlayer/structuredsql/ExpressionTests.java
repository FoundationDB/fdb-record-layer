/*
 * ExpressionTests.java
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

package com.apple.foundationdb.relational.recordlayer.structuredsql;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.structuredsql.expression.ExpressionFactoryImpl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

@API(API.Status.EXPERIMENTAL)
public class ExpressionTests {

    @Nonnull
    private static SchemaTemplate sampleSchemaTemplate() {
        return RecordLayerSchemaTemplate.newBuilder()
                .setName("TestSchemaTemplate")
                .setVersion(42)
                .addAuxiliaryType(DataType.StructType.from(
                        "Subtype",
                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 0)),
                        true))
                .addTable(
                        RecordLayerTable.newBuilder(false)
                                .setName("T1")
                                .addColumn(RecordLayerColumn.newBuilder()
                                        .setName("col1")
                                        .setDataType(
                                                DataType.StructType.from(
                                                        "Subtype",
                                                        List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                                                        true))
                                        .build())
                                .build())
                .build();
    }

    @Test
    public void testLiteral() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        Assertions.assertThat(expressionFactory.literal(false).getValue()).isEqualTo(false);
        Assertions.assertThat(expressionFactory.literal(true).getValue()).isEqualTo(true);
        Assertions.assertThat(expressionFactory.literal((Boolean) null).getValue()).isNull();
        Assertions.assertThat(expressionFactory.literal(42).getValue()).isEqualTo(42);
        Assertions.assertThat(expressionFactory.literal((Integer) null).getValue()).isNull();
        Assertions.assertThat(expressionFactory.literal(42L).getValue()).isEqualTo(42L);
        Assertions.assertThat(expressionFactory.literal((Long) null).getValue()).isNull();
        Assertions.assertThat(expressionFactory.literal(42.0f).getValue()).isEqualTo(42.0f);
        Assertions.assertThat(expressionFactory.literal((Float) null).getValue()).isNull();
        Assertions.assertThat(expressionFactory.literal(42.1d).getValue()).isEqualTo(42.1d);
        Assertions.assertThat(expressionFactory.literal((Double) null).getValue()).isNull();
        Assertions.assertThat(expressionFactory.literal("hello").getValue()).isEqualTo("hello");
        Assertions.assertThat(expressionFactory.literal((String) null).getValue()).isNull();
    }

    @Test
    public void nonExistingFieldResolution() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        Assertions.assertThatThrownBy(() -> expressionFactory.field("non-existing", List.of("whatever")))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("Could not find table 'NON-EXISTING'");
        Assertions.assertThatThrownBy(() -> expressionFactory.field("T1", List.of("non-existing")))
                .isInstanceOf(UncheckedRelationalException.class)
                .hasMessageContaining("invalid field reference 'non-existing'");
    }

    @Test
    public void fieldResolution() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var field = expressionFactory.field("T1", "\"col1\"");
        Assertions.assertThat(field).isNotNull();
        Assertions.assertThat(field.getName()).isEqualTo("col1");
        Assertions.assertThat(field.getType()).isEqualTo(DataType.StructType.from(
                "Subtype",
                List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                true));
    }

    @Test
    public void fieldResolutionWithCaseSensitivity() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(),
                Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var field = expressionFactory.field("T1", "col1");
        Assertions.assertThat(field).isNotNull();
        Assertions.assertThat(field.getName()).isEqualTo("col1");
        Assertions.assertThat(field.getType()).isEqualTo(DataType.StructType.from(
                "Subtype",
                List.of(DataType.StructType.Field.from("field1", DataType.Primitives.INTEGER.type(), 1)),
                true));
    }

    @Test
    public void nestedFieldResolution() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var nestedField = expressionFactory.field("T1", List.of("\"col1\"", "\"field1\""));
        Assertions.assertThat(nestedField).isNotNull();
        Assertions.assertThat(nestedField.getName()).isEqualTo("field1");
        Assertions.assertThat(nestedField.getType()).isEqualTo(DataType.Primitives.INTEGER.type());
    }

    @Test
    public void nestedFieldResolutionCaseSensitivity() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(),
                Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var nestedField = expressionFactory.field("T1", List.of("col1", "field1"));
        Assertions.assertThat(nestedField).isNotNull();
        Assertions.assertThat(nestedField.getName()).isEqualTo("field1");
        Assertions.assertThat(nestedField.getType()).isEqualTo(DataType.Primitives.INTEGER.type());
    }
}
