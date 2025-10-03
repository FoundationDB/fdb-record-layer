/*
 * DataTypeStringTest.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.metadata.DataType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for validating features of the {@link DataType} class.
 */
class DataTypeSmokeTest {
    private static final String OR_NULL = " ∪ ∅";

    @Nonnull
    static Stream<Arguments> assertStringMatches() {
        final DataType.StructType baseStructType = DataType.StructType.from("sample_type",
                List.of(
                        DataType.StructType.Field.from("a", DataType.LongType.nullable(), 1),
                        DataType.StructType.Field.from("b", DataType.StringType.notNullable(), 2)
                ),
                false);

        final DataType.StructType structWithNested = DataType.StructType.from("par",
                List.of(
                        DataType.StructType.Field.from("x", baseStructType.withNullable(false), 1),
                        DataType.StructType.Field.from("y", baseStructType.withNullable(true), 2)
                ),
                false);

        final DataType.EnumType suitsEnum = DataType.EnumType.from("suits",
                List.of(
                        DataType.EnumType.EnumValue.of("SPADES", 0),
                        DataType.EnumType.EnumValue.of("HEARTS", 1),
                        DataType.EnumType.EnumValue.of("CLUBS", 2),
                        DataType.EnumType.EnumValue.of("DIAMONDS", 3)
                ), false);

        return Stream.of(
                // Primite types
                Arguments.of(DataType.NullType.nullable(), "∅"),
                Arguments.of(DataType.BooleanType.notNullable(), "boolean"),
                Arguments.of(DataType.BooleanType.nullable(), "boolean" + OR_NULL),
                Arguments.of(DataType.LongType.notNullable(), "long"),
                Arguments.of(DataType.LongType.nullable(), "long" + OR_NULL),
                Arguments.of(DataType.IntegerType.notNullable(), "int"),
                Arguments.of(DataType.IntegerType.nullable(), "int" + OR_NULL),
                Arguments.of(DataType.FloatType.notNullable(), "float"),
                Arguments.of(DataType.FloatType.nullable(), "float" + OR_NULL),
                Arguments.of(DataType.DoubleType.notNullable(), "double"),
                Arguments.of(DataType.DoubleType.nullable(), "double" + OR_NULL),
                Arguments.of(DataType.StringType.notNullable(), "string"),
                Arguments.of(DataType.StringType.nullable(), "string" + OR_NULL),
                Arguments.of(DataType.BytesType.notNullable(), "bytes"),
                Arguments.of(DataType.BytesType.nullable(), "bytes" + OR_NULL),
                Arguments.of(DataType.UuidType.notNullable(), "uuid"),
                Arguments.of(DataType.UuidType.nullable(), "uuid" + OR_NULL),
                Arguments.of(DataType.VersionType.notNullable(), "version"),
                Arguments.of(DataType.VersionType.nullable(), "version" + OR_NULL),

                // Arrays
                Arguments.of(DataType.ArrayType.from(DataType.StringType.notNullable()), "[string]"),
                Arguments.of(DataType.ArrayType.from(DataType.StringType.nullable()), "[string" + OR_NULL + "]"),
                Arguments.of(DataType.ArrayType.from(DataType.StringType.notNullable(), true), "[string]" + OR_NULL),
                Arguments.of(DataType.ArrayType.from(DataType.StringType.nullable(), true), "[string" + OR_NULL + "]" + OR_NULL),

                // Structs
                Arguments.of(baseStructType.withNullable(false), "sampl { a:long" + OR_NULL + ",b:string } "),
                Arguments.of(baseStructType.withNullable(true), "sampl { a:long" + OR_NULL + ",b:string } "),
                Arguments.of(structWithNested.withNullable(false), "par { x:sampl { a:long" + OR_NULL + ",b:string } ,y:sampl { a:long" + OR_NULL + ",b:string }  } "),
                Arguments.of(structWithNested.withNullable(true), "par { x:sampl { a:long" + OR_NULL + ",b:string } ,y:sampl { a:long" + OR_NULL + ",b:string }  } "),

                // Enums
                Arguments.of(suitsEnum.withNullable(false), "enum(suits){SPADES,HEARTS,CLUBS,DIAMONDS}"),
                Arguments.of(suitsEnum.withNullable(true), "enum(suits){SPADES,HEARTS,CLUBS,DIAMONDS}"),

                Arguments.of(DataType.UnknownType.instance(), "???")
        );
    }

    @ParameterizedTest(name = "assertStringMatches[dataType={0}]")
    @MethodSource
    void assertStringMatches(@Nonnull DataType dataType, @Nonnull String string) {
        assertThat(dataType)
                .hasToString(string);
    }
}
