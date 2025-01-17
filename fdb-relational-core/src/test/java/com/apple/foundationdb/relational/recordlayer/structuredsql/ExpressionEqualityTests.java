/*
 * ExpressionEqualityTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.Options;
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

import static org.junit.jupiter.api.Assertions.assertSame;

public class ExpressionEqualityTests {
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
                                .addColumn(RecordLayerColumn.newBuilder().setName("col1").setDataType(DataType.Primitives.BOOLEAN.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("col2").setDataType(DataType.Primitives.INTEGER.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("col3").setDataType(DataType.Primitives.LONG.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("col4").setDataType(DataType.Primitives.FLOAT.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("col5").setDataType(DataType.Primitives.DOUBLE.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("col6").setDataType(DataType.Primitives.STRING.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol1").setDataType(DataType.Primitives.NULLABLE_BOOLEAN.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol2").setDataType(DataType.Primitives.NULLABLE_INTEGER.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol3").setDataType(DataType.Primitives.NULLABLE_LONG.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol4").setDataType(DataType.Primitives.NULLABLE_FLOAT.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol5").setDataType(DataType.Primitives.NULLABLE_DOUBLE.type()).build())
                                .addColumn(RecordLayerColumn.newBuilder().setName("nullableCol6").setDataType(DataType.Primitives.NULLABLE_STRING.type()).build())
                                .build())
                .build();
    }

    @Test
    public void testBooleanParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsBoolean = parsingExp1.asBoolean();
        Assertions.assertThat(parsingExp1AsBoolean).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsBoolean).isEqualTo(parsingExp1AsBoolean);

        final var parsingExp2AsBoolean = parsingExp1.asBoolean();
        Assertions.assertThat(parsingExp2AsBoolean).isEqualTo(parsingExp1AsBoolean);
        Assertions.assertThat(parsingExp1AsBoolean).isEqualTo(parsingExp2AsBoolean);

        assertSame(parsingExp1AsBoolean, parsingExp1AsBoolean.asBoolean().asBoolean().asBoolean().asBoolean());

        Assertions.assertThat(parsingExp1AsBoolean.hashCode()).isEqualTo(parsingExp2AsBoolean.hashCode());
        Assertions.assertThat(parsingExp1AsBoolean.hashCode()).isEqualTo(parsingExp1AsBoolean.hashCode());

        final var parsingExprOtherAsBoolean = parsingExpOther.asBoolean();
        Assertions.assertThat(parsingExprOtherAsBoolean).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testIntParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsInt = parsingExp1.asInt();
        Assertions.assertThat(parsingExp1AsInt).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsInt).isEqualTo(parsingExp1AsInt);

        final var parsingExp2AsInt = parsingExp1.asInt();
        Assertions.assertThat(parsingExp2AsInt).isEqualTo(parsingExp1AsInt);
        Assertions.assertThat(parsingExp1AsInt).isEqualTo(parsingExp2AsInt);

        assertSame(parsingExp1AsInt, parsingExp1AsInt.asInt().asInt().asInt().asInt());

        Assertions.assertThat(parsingExp1AsInt.hashCode()).isEqualTo(parsingExp2AsInt.hashCode());
        Assertions.assertThat(parsingExp1AsInt.hashCode()).isEqualTo(parsingExp1AsInt.hashCode());

        final var parsingExprOtherAsInt = parsingExpOther.asInt();
        Assertions.assertThat(parsingExprOtherAsInt).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testLongParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsLong = parsingExp1.asLong();
        Assertions.assertThat(parsingExp1AsLong).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsLong).isEqualTo(parsingExp1AsLong);

        final var parsingExp2AsLong = parsingExp1.asLong();
        Assertions.assertThat(parsingExp2AsLong).isEqualTo(parsingExp1AsLong);
        Assertions.assertThat(parsingExp1AsLong).isEqualTo(parsingExp2AsLong);

        assertSame(parsingExp1AsLong, parsingExp1AsLong.asLong().asLong().asLong().asLong());

        Assertions.assertThat(parsingExp1AsLong.hashCode()).isEqualTo(parsingExp2AsLong.hashCode());
        Assertions.assertThat(parsingExp1AsLong.hashCode()).isEqualTo(parsingExp1AsLong.hashCode());

        final var parsingExprOtherAsLong = parsingExpOther.asLong();
        Assertions.assertThat(parsingExprOtherAsLong).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testFloatParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsFloat = parsingExp1.asFloat();
        Assertions.assertThat(parsingExp1AsFloat).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsFloat).isEqualTo(parsingExp1AsFloat);

        final var parsingExp2AsFloat = parsingExp1.asFloat();
        Assertions.assertThat(parsingExp2AsFloat).isEqualTo(parsingExp1AsFloat);
        Assertions.assertThat(parsingExp1AsFloat).isEqualTo(parsingExp2AsFloat);

        assertSame(parsingExp1AsFloat, parsingExp1AsFloat.asFloat().asFloat().asFloat().asFloat());

        Assertions.assertThat(parsingExp1AsFloat.hashCode()).isEqualTo(parsingExp2AsFloat.hashCode());
        Assertions.assertThat(parsingExp1AsFloat.hashCode()).isEqualTo(parsingExp1AsFloat.hashCode());

        final var parsingExprOtherAsFloat = parsingExpOther.asFloat();
        Assertions.assertThat(parsingExprOtherAsFloat).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testDoubleParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsDouble = parsingExp1.asDouble();
        Assertions.assertThat(parsingExp1AsDouble).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsDouble).isEqualTo(parsingExp1AsDouble);

        final var parsingExp2AsDouble = parsingExp1.asDouble();
        Assertions.assertThat(parsingExp2AsDouble).isEqualTo(parsingExp1AsDouble);
        Assertions.assertThat(parsingExp1AsDouble).isEqualTo(parsingExp2AsDouble);

        assertSame(parsingExp1AsDouble, parsingExp1AsDouble.asDouble().asDouble().asDouble().asDouble());

        Assertions.assertThat(parsingExp1AsDouble.hashCode()).isEqualTo(parsingExp2AsDouble.hashCode());
        Assertions.assertThat(parsingExp1AsDouble.hashCode()).isEqualTo(parsingExp1AsDouble.hashCode());

        final var parsingExprOtherAsDouble = parsingExpOther.asDouble();
        Assertions.assertThat(parsingExprOtherAsDouble).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testStringParseExpressionEquality() {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.NONE);
        final var parsingExp1 = expressionFactory.parseFragment("bla");
        final var parsingExp2 = expressionFactory.parseFragment("bla");

        final var parsingExpOther = expressionFactory.parseFragment("bar");

        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp2.hashCode());
        Assertions.assertThat(parsingExp1.hashCode()).isEqualTo(parsingExp1.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isEqualTo(parsingExp2);
        Assertions.assertThat(parsingExp2).isEqualTo(parsingExp1);
        Assertions.assertThat(parsingExp1).isNotEqualTo(parsingExpOther);

        final var parsingExp1AsString = parsingExp1.asString();
        Assertions.assertThat(parsingExp1AsString).isNotEqualTo(parsingExp1);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsingExp1AsString).isEqualTo(parsingExp1AsString);

        final var parsingExp2AsString = parsingExp1.asString();
        Assertions.assertThat(parsingExp2AsString).isEqualTo(parsingExp1AsString);
        Assertions.assertThat(parsingExp1AsString).isEqualTo(parsingExp2AsString);

        assertSame(parsingExp1AsString, parsingExp1AsString.asString().asString().asString().asString());

        Assertions.assertThat(parsingExp1AsString.hashCode()).isEqualTo(parsingExp2AsString.hashCode());
        Assertions.assertThat(parsingExp1AsString.hashCode()).isEqualTo(parsingExp1AsString.hashCode());

        final var parsingExprOtherAsString = parsingExpOther.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsingExp1);
    }

    @Test
    public void testBooleanFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedBooleanField = expressionFactory.field("T1", "col1");
        final var userDefinedBooleanField = expressionFactory.field(DataType.BooleanType.notNullable(), "col1");
        final var parsedNullableBooleanField = expressionFactory.field("T1", "nullableCol1");
        final var userDefinedNullableBooleanField = expressionFactory.field(DataType.BooleanType.nullable(), "nullableCol1");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedBooleanField.hashCode()).isEqualTo(userDefinedBooleanField.hashCode());
        Assertions.assertThat(parsedBooleanField.hashCode()).isEqualTo(parsedBooleanField.hashCode());

        Assertions.assertThat(parsedNullableBooleanField.hashCode()).isEqualTo(userDefinedNullableBooleanField.hashCode());
        Assertions.assertThat(parsedNullableBooleanField.hashCode()).isEqualTo(parsedNullableBooleanField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedBooleanField).isEqualTo(parsedBooleanField);
        Assertions.assertThat(parsedBooleanField).isEqualTo(userDefinedBooleanField);
        Assertions.assertThat(userDefinedBooleanField).isEqualTo(parsedBooleanField);
        Assertions.assertThat(parsedBooleanField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableBooleanField).isEqualTo(parsedNullableBooleanField);
        Assertions.assertThat(parsedNullableBooleanField).isEqualTo(userDefinedNullableBooleanField);
        Assertions.assertThat(userDefinedNullableBooleanField).isEqualTo(parsedNullableBooleanField);
        Assertions.assertThat(parsedNullableBooleanField).isNotEqualTo(otherField);

        final var parsedBooleanFieldAsString = parsedBooleanField.asString();
        Assertions.assertThat(parsedBooleanFieldAsString).isNotEqualTo(parsedBooleanField);
        Assertions.assertThat(parsedBooleanFieldAsString).isNotEqualTo(parsedNullableBooleanField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedBooleanFieldAsString).isEqualTo(parsedBooleanFieldAsString);

        final var userDefinedBooleanFieldAsString = parsedBooleanField.asString();
        Assertions.assertThat(userDefinedBooleanFieldAsString).isEqualTo(parsedBooleanFieldAsString);
        Assertions.assertThat(parsedBooleanFieldAsString).isEqualTo(userDefinedBooleanFieldAsString);
        Assertions.assertThat(userDefinedBooleanFieldAsString).isEqualTo(parsedBooleanFieldAsString);
        Assertions.assertThat(parsedBooleanFieldAsString).isEqualTo(userDefinedBooleanFieldAsString);

        assertSame(parsedBooleanFieldAsString, parsedBooleanFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedBooleanFieldAsString.hashCode()).isEqualTo(userDefinedBooleanFieldAsString.hashCode());
        Assertions.assertThat(parsedBooleanFieldAsString.hashCode()).isEqualTo(parsedBooleanFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedBooleanField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableBooleanField);
    }

    @Test
    public void testIntFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedIntField = expressionFactory.field("T1", "col2");
        final var userDefinedIntField = expressionFactory.field(DataType.IntegerType.notNullable(), "col2");
        final var parsedNullableIntField = expressionFactory.field("T1", "nullableCol2");
        final var userDefinedNullableIntField = expressionFactory.field(DataType.IntegerType.nullable(), "nullableCol2");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedIntField.hashCode()).isEqualTo(userDefinedIntField.hashCode());
        Assertions.assertThat(parsedIntField.hashCode()).isEqualTo(parsedIntField.hashCode());

        Assertions.assertThat(parsedNullableIntField.hashCode()).isEqualTo(userDefinedNullableIntField.hashCode());
        Assertions.assertThat(parsedNullableIntField.hashCode()).isEqualTo(parsedNullableIntField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedIntField).isEqualTo(parsedIntField);
        Assertions.assertThat(parsedIntField).isEqualTo(userDefinedIntField);
        Assertions.assertThat(userDefinedIntField).isEqualTo(parsedIntField);
        Assertions.assertThat(parsedIntField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableIntField).isEqualTo(parsedNullableIntField);
        Assertions.assertThat(parsedNullableIntField).isEqualTo(userDefinedNullableIntField);
        Assertions.assertThat(userDefinedNullableIntField).isEqualTo(parsedNullableIntField);
        Assertions.assertThat(parsedNullableIntField).isNotEqualTo(otherField);

        final var parsedIntFieldAsString = parsedIntField.asString();
        Assertions.assertThat(parsedIntFieldAsString).isNotEqualTo(parsedIntField);
        Assertions.assertThat(parsedIntFieldAsString).isNotEqualTo(parsedNullableIntField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedIntFieldAsString).isEqualTo(parsedIntFieldAsString);

        final var userDefinedIntFieldAsString = parsedIntField.asString();
        Assertions.assertThat(userDefinedIntFieldAsString).isEqualTo(parsedIntFieldAsString);
        Assertions.assertThat(parsedIntFieldAsString).isEqualTo(userDefinedIntFieldAsString);
        Assertions.assertThat(userDefinedIntFieldAsString).isEqualTo(parsedIntFieldAsString);
        Assertions.assertThat(parsedIntFieldAsString).isEqualTo(userDefinedIntFieldAsString);

        assertSame(parsedIntFieldAsString, parsedIntFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedIntFieldAsString.hashCode()).isEqualTo(userDefinedIntFieldAsString.hashCode());
        Assertions.assertThat(parsedIntFieldAsString.hashCode()).isEqualTo(parsedIntFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedIntField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableIntField);
    }

    @Test
    public void testLongFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedLongField = expressionFactory.field("T1", "col3");
        final var userDefinedLongField = expressionFactory.field(DataType.LongType.notNullable(), "col3");
        final var parsedNullableLongField = expressionFactory.field("T1", "nullableCol3");
        final var userDefinedNullableLongField = expressionFactory.field(DataType.LongType.nullable(), "nullableCol3");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedLongField.hashCode()).isEqualTo(userDefinedLongField.hashCode());
        Assertions.assertThat(parsedLongField.hashCode()).isEqualTo(parsedLongField.hashCode());

        Assertions.assertThat(parsedNullableLongField.hashCode()).isEqualTo(userDefinedNullableLongField.hashCode());
        Assertions.assertThat(parsedNullableLongField.hashCode()).isEqualTo(parsedNullableLongField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedLongField).isEqualTo(parsedLongField);
        Assertions.assertThat(parsedLongField).isEqualTo(userDefinedLongField);
        Assertions.assertThat(userDefinedLongField).isEqualTo(parsedLongField);
        Assertions.assertThat(parsedLongField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableLongField).isEqualTo(parsedNullableLongField);
        Assertions.assertThat(parsedNullableLongField).isEqualTo(userDefinedNullableLongField);
        Assertions.assertThat(userDefinedNullableLongField).isEqualTo(parsedNullableLongField);
        Assertions.assertThat(parsedNullableLongField).isNotEqualTo(otherField);

        final var parsedLongFieldAsString = parsedLongField.asString();
        Assertions.assertThat(parsedLongFieldAsString).isNotEqualTo(parsedLongField);
        Assertions.assertThat(parsedLongFieldAsString).isNotEqualTo(parsedNullableLongField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedLongFieldAsString).isEqualTo(parsedLongFieldAsString);

        final var userDefinedLongFieldAsString = parsedLongField.asString();
        Assertions.assertThat(userDefinedLongFieldAsString).isEqualTo(parsedLongFieldAsString);
        Assertions.assertThat(parsedLongFieldAsString).isEqualTo(userDefinedLongFieldAsString);
        Assertions.assertThat(userDefinedLongFieldAsString).isEqualTo(parsedLongFieldAsString);
        Assertions.assertThat(parsedLongFieldAsString).isEqualTo(userDefinedLongFieldAsString);

        assertSame(parsedLongFieldAsString, parsedLongFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedLongFieldAsString.hashCode()).isEqualTo(userDefinedLongFieldAsString.hashCode());
        Assertions.assertThat(parsedLongFieldAsString.hashCode()).isEqualTo(parsedLongFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedLongField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableLongField);
    }

    @Test
    public void testFloatFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedFloatField = expressionFactory.field("T1", "col4");
        final var userDefinedFloatField = expressionFactory.field(DataType.FloatType.notNullable(), "col4");
        final var parsedNullableFloatField = expressionFactory.field("T1", "nullableCol4");
        final var userDefinedNullableFloatField = expressionFactory.field(DataType.FloatType.nullable(), "nullableCol4");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedFloatField.hashCode()).isEqualTo(userDefinedFloatField.hashCode());
        Assertions.assertThat(parsedFloatField.hashCode()).isEqualTo(parsedFloatField.hashCode());

        Assertions.assertThat(parsedNullableFloatField.hashCode()).isEqualTo(userDefinedNullableFloatField.hashCode());
        Assertions.assertThat(parsedNullableFloatField.hashCode()).isEqualTo(parsedNullableFloatField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedFloatField).isEqualTo(parsedFloatField);
        Assertions.assertThat(parsedFloatField).isEqualTo(userDefinedFloatField);
        Assertions.assertThat(userDefinedFloatField).isEqualTo(parsedFloatField);
        Assertions.assertThat(parsedFloatField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableFloatField).isEqualTo(parsedNullableFloatField);
        Assertions.assertThat(parsedNullableFloatField).isEqualTo(userDefinedNullableFloatField);
        Assertions.assertThat(userDefinedNullableFloatField).isEqualTo(parsedNullableFloatField);
        Assertions.assertThat(parsedNullableFloatField).isNotEqualTo(otherField);

        final var parsedFloatFieldAsString = parsedFloatField.asString();
        Assertions.assertThat(parsedFloatFieldAsString).isNotEqualTo(parsedFloatField);
        Assertions.assertThat(parsedFloatFieldAsString).isNotEqualTo(parsedNullableFloatField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedFloatFieldAsString).isEqualTo(parsedFloatFieldAsString);

        final var userDefinedFloatFieldAsString = parsedFloatField.asString();
        Assertions.assertThat(userDefinedFloatFieldAsString).isEqualTo(parsedFloatFieldAsString);
        Assertions.assertThat(parsedFloatFieldAsString).isEqualTo(userDefinedFloatFieldAsString);
        Assertions.assertThat(userDefinedFloatFieldAsString).isEqualTo(parsedFloatFieldAsString);
        Assertions.assertThat(parsedFloatFieldAsString).isEqualTo(userDefinedFloatFieldAsString);

        assertSame(parsedFloatFieldAsString, parsedFloatFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedFloatFieldAsString.hashCode()).isEqualTo(userDefinedFloatFieldAsString.hashCode());
        Assertions.assertThat(parsedFloatFieldAsString.hashCode()).isEqualTo(parsedFloatFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedFloatField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableFloatField);
    }

    @Test
    public void testDoubleFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedDoubleField = expressionFactory.field("T1", "col5");
        final var userDefinedDoubleField = expressionFactory.field(DataType.DoubleType.notNullable(), "col5");
        final var parsedNullableDoubleField = expressionFactory.field("T1", "nullableCol5");
        final var userDefinedNullableDoubleField = expressionFactory.field(DataType.DoubleType.nullable(), "nullableCol5");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedDoubleField.hashCode()).isEqualTo(userDefinedDoubleField.hashCode());
        Assertions.assertThat(parsedDoubleField.hashCode()).isEqualTo(parsedDoubleField.hashCode());

        Assertions.assertThat(parsedNullableDoubleField.hashCode()).isEqualTo(userDefinedNullableDoubleField.hashCode());
        Assertions.assertThat(parsedNullableDoubleField.hashCode()).isEqualTo(parsedNullableDoubleField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedDoubleField).isEqualTo(parsedDoubleField);
        Assertions.assertThat(parsedDoubleField).isEqualTo(userDefinedDoubleField);
        Assertions.assertThat(userDefinedDoubleField).isEqualTo(parsedDoubleField);
        Assertions.assertThat(parsedDoubleField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableDoubleField).isEqualTo(parsedNullableDoubleField);
        Assertions.assertThat(parsedNullableDoubleField).isEqualTo(userDefinedNullableDoubleField);
        Assertions.assertThat(userDefinedNullableDoubleField).isEqualTo(parsedNullableDoubleField);
        Assertions.assertThat(parsedNullableDoubleField).isNotEqualTo(otherField);

        final var parsedDoubleFieldAsString = parsedDoubleField.asString();
        Assertions.assertThat(parsedDoubleFieldAsString).isNotEqualTo(parsedDoubleField);
        Assertions.assertThat(parsedDoubleFieldAsString).isNotEqualTo(parsedNullableDoubleField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedDoubleFieldAsString).isEqualTo(parsedDoubleFieldAsString);

        final var userDefinedDoubleFieldAsString = parsedDoubleField.asString();
        Assertions.assertThat(userDefinedDoubleFieldAsString).isEqualTo(parsedDoubleFieldAsString);
        Assertions.assertThat(parsedDoubleFieldAsString).isEqualTo(userDefinedDoubleFieldAsString);
        Assertions.assertThat(userDefinedDoubleFieldAsString).isEqualTo(parsedDoubleFieldAsString);
        Assertions.assertThat(parsedDoubleFieldAsString).isEqualTo(userDefinedDoubleFieldAsString);

        assertSame(parsedDoubleFieldAsString, parsedDoubleFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedDoubleFieldAsString.hashCode()).isEqualTo(userDefinedDoubleFieldAsString.hashCode());
        Assertions.assertThat(parsedDoubleFieldAsString.hashCode()).isEqualTo(parsedDoubleFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedDoubleField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableDoubleField);
    }

    @Test
    public void testStringFieldEquality() throws Exception {
        final var expressionFactory = new ExpressionFactoryImpl(sampleSchemaTemplate(), Options.builder().withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, true).build());
        final var parsedStringField = expressionFactory.field("T1", "col6");
        final var userDefinedStringField = expressionFactory.field(DataType.StringType.notNullable(), "col6");
        final var parsedNullableStringField = expressionFactory.field("T1", "nullableCol6");
        final var userDefinedNullableStringField = expressionFactory.field(DataType.StringType.nullable(), "nullableCol6");

        final var otherField = expressionFactory.field(DataType.UnknownType.instance(), "whatever");

        Assertions.assertThat(parsedStringField.hashCode()).isEqualTo(userDefinedStringField.hashCode());
        Assertions.assertThat(parsedStringField.hashCode()).isEqualTo(parsedStringField.hashCode());

        Assertions.assertThat(parsedNullableStringField.hashCode()).isEqualTo(userDefinedNullableStringField.hashCode());
        Assertions.assertThat(parsedNullableStringField.hashCode()).isEqualTo(parsedNullableStringField.hashCode());

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedStringField).isEqualTo(parsedStringField);
        Assertions.assertThat(parsedStringField).isEqualTo(userDefinedStringField);
        Assertions.assertThat(userDefinedStringField).isEqualTo(parsedStringField);
        Assertions.assertThat(parsedStringField).isNotEqualTo(otherField);

        //noinspection EqualsWithItself
        Assertions.assertThat(parsedNullableStringField).isEqualTo(parsedNullableStringField);
        Assertions.assertThat(parsedNullableStringField).isEqualTo(userDefinedNullableStringField);
        Assertions.assertThat(userDefinedNullableStringField).isEqualTo(parsedNullableStringField);
        Assertions.assertThat(parsedNullableStringField).isNotEqualTo(otherField);

        final var parsedStringFieldAsString = parsedStringField.asString();
        Assertions.assertThat(parsedStringFieldAsString).isNotEqualTo(parsedStringField);
        Assertions.assertThat(parsedStringFieldAsString).isNotEqualTo(parsedNullableStringField);
        //noinspection EqualsWithItself
        Assertions.assertThat(parsedStringFieldAsString).isEqualTo(parsedStringFieldAsString);

        final var userDefinedStringFieldAsString = parsedStringField.asString();
        Assertions.assertThat(userDefinedStringFieldAsString).isEqualTo(parsedStringFieldAsString);
        Assertions.assertThat(parsedStringFieldAsString).isEqualTo(userDefinedStringFieldAsString);
        Assertions.assertThat(userDefinedStringFieldAsString).isEqualTo(parsedStringFieldAsString);
        Assertions.assertThat(parsedStringFieldAsString).isEqualTo(userDefinedStringFieldAsString);

        assertSame(parsedStringFieldAsString, parsedStringFieldAsString.asString().asString().asString().asString());

        Assertions.assertThat(parsedStringFieldAsString.hashCode()).isEqualTo(userDefinedStringFieldAsString.hashCode());
        Assertions.assertThat(parsedStringFieldAsString.hashCode()).isEqualTo(parsedStringFieldAsString.hashCode());

        final var parsingExprOtherAsString = otherField.asString();
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedStringField);
        Assertions.assertThat(parsingExprOtherAsString).isNotEqualTo(parsedNullableStringField);
    }
}
