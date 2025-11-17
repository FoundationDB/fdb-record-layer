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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RecordLayerColumn}.
 */
class RecordLayerColumnTests {

    @Test
    void basicBuilder() {
        final RecordLayerColumn column = RecordLayerColumn.newBuilder()
                .setName("blah")
                .setDataType(DataType.LongType.nullable())
                .setIndex(1)
                .build();
        assertThat(column)
                .hasToString("blah: long ∪ ∅ = 1");

        final RecordLayerColumn copy = RecordLayerColumn.newBuilder()
                .setName(column.getName())
                .setDataType(column.getDataType())
                .setIndex(column.getIndex())
                .build();
        assertThat(copy)
                .hasToString("blah: long ∪ ∅ = 1")
                .isEqualTo(column)
                .hasSameHashCodeAs(column);

        final RecordLayerColumn differentName = RecordLayerColumn.newBuilder()
                .setName("blag")
                .setDataType(column.getDataType())
                .setIndex(column.getIndex())
                .build();
        assertThat(differentName)
                .hasToString("blag: long ∪ ∅ = 1")
                .isNotEqualTo(column)
                .doesNotHaveSameHashCodeAs(column);

        final RecordLayerColumn differentType1 = RecordLayerColumn.newBuilder()
                .setName(column.getName())
                .setDataType(DataType.LongType.notNullable())
                .setIndex(column.getIndex())
                .build();
        assertThat(differentType1)
                .hasToString("blah: long = 1")
                .isNotEqualTo(column)
                .doesNotHaveSameHashCodeAs(column);

        final RecordLayerColumn differentType2 = RecordLayerColumn.newBuilder()
                .setName(column.getName())
                .setDataType(DataType.StringType.nullable())
                .setIndex(column.getIndex())
                .build();
        assertThat(differentType2)
                .hasToString("blah: string ∪ ∅ = 1")
                .isNotEqualTo(column)
                .doesNotHaveSameHashCodeAs(column);

        final RecordLayerColumn differentIndex = RecordLayerColumn.newBuilder()
                .setName(column.getName())
                .setDataType(column.getDataType())
                .setIndex(column.getIndex() + 1)
                .build();
        assertThat(differentIndex)
                .hasToString("blah: long ∪ ∅ = 2")
                .isNotEqualTo(column)
                .doesNotHaveSameHashCodeAs(column);
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
