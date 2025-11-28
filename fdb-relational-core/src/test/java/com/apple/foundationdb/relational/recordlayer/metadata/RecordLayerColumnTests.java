/*
 * RecordLayerColumnTests.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.metadata.DataType;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RecordLayerColumn}.
 */
class RecordLayerColumnTests {
    private static final RecordLayerColumn BLAH_COLUMN = RecordLayerColumn.newBuilder()
            .setName("blah")
            .setDataType(DataType.LongType.nullable())
            .setIndex(1)
            .build();

    @Test
    void basicCopy() {
        final RecordLayerColumn copy = RecordLayerColumn.newBuilder()
                .setName(BLAH_COLUMN.getName())
                .setDataType(BLAH_COLUMN.getDataType())
                .setIndex(BLAH_COLUMN.getIndex())
                .build();
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(copy)
                    .hasToString("blah: long ∪ ∅ = 1");
            softly.assertThat(BLAH_COLUMN)
                    .hasToString("blah: long ∪ ∅ = 1");
            softly.assertThat(copy.getName())
                    .isEqualTo("blah");
            softly.assertThat(copy.getDataType())
                    .isEqualTo(DataType.LongType.nullable());
            softly.assertThat(copy.getIndex())
                    .isOne();
        }
    }

    @Nonnull
    static Stream<Arguments> testDiffersFromBlah() {
        return Stream.of(
                Arguments.of(
                        // Different name
                        RecordLayerColumn.newBuilder()
                                .setName("blag")
                                .setDataType(BLAH_COLUMN.getDataType())
                                .setIndex(BLAH_COLUMN.getIndex())
                                .build(),
                        "blag: long ∪ ∅ = 1"
                ),
                Arguments.of(
                        // Different type 1
                        RecordLayerColumn.newBuilder()
                                .setName(BLAH_COLUMN.getName())
                                .setDataType(DataType.LongType.notNullable())
                                .setIndex(BLAH_COLUMN.getIndex())
                                .build(),
                        "blah: long = 1"
                ),
                Arguments.of(
                        // Different type 2
                        RecordLayerColumn.newBuilder()
                                .setName(BLAH_COLUMN.getName())
                                .setDataType(DataType.StringType.nullable())
                                .setIndex(BLAH_COLUMN.getIndex())
                                .build(),
                        "blah: string ∪ ∅ = 1"
                ),
                Arguments.of(
                        // Different index
                        RecordLayerColumn.newBuilder()
                                .setName(BLAH_COLUMN.getName())
                                .setDataType(BLAH_COLUMN.getDataType())
                                .setIndex(BLAH_COLUMN.getIndex() + 1)
                                .build(),
                        "blah: long ∪ ∅ = 2"
                )
        );
    }

    @ParameterizedTest(name = "testDiffersFromBlah[{1}]")
    @MethodSource
    void testDiffersFromBlah(@Nonnull RecordLayerColumn differentColumn, @Nonnull String toString) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(differentColumn)
                    .hasToString(toString)
                    .isNotEqualTo(BLAH_COLUMN)
                    .doesNotHaveSameHashCodeAs(BLAH_COLUMN)
                    .matches(c -> !BLAH_COLUMN.getName().equals(c.getName()) || !BLAH_COLUMN.getDataType().equals(c.getDataType()) || BLAH_COLUMN.getIndex() != c.getIndex());
        }
    }

    @Test
    void fromStructField() {
        final RecordLayerColumn columnFromStructType = RecordLayerColumn.from(DataType.StructType.Field.from("blah", DataType.LongType.nullable(), 1));
        assertThat(columnFromStructType)
                .hasToString("blah: long ∪ ∅ = 1");

        final RecordLayerColumn columnFromBuilder = RecordLayerColumn.newBuilder()
                .setName(columnFromStructType.getName())
                .setDataType(columnFromStructType.getDataType())
                .setIndex(columnFromStructType.getIndex())
                .build();
        assertThat(columnFromBuilder)
                .hasToString("blah: long ∪ ∅ = 1")
                .isEqualTo(columnFromStructType)
                .hasSameHashCodeAs(columnFromStructType);
    }
}
