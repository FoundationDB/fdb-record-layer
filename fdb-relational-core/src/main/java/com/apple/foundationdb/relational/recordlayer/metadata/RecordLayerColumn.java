/*
 * RecordLayerColumn.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.metadata.Column;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerColumn implements Column {
    private final String name;

    private final DataType dataType;

    private final int index;

    RecordLayerColumn(String name, DataType dataType, int index) {
        this.name = name;
        this.dataType = dataType;
        this.index = index;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        final RecordLayerColumn that = (RecordLayerColumn)object;
        return index == that.index && Objects.equals(name, that.name) && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, index);
    }

    @Override
    public String toString() {
        return name + ": " + dataType + " = " + index;
    }

    public static final class Builder {
        private String name;
        private DataType dataType;

        private int index;

        // name and dataType are populated by the fluent setters below, so NullAway cannot see
        // that they are always set before use in build().
        @SuppressWarnings("NullAway.Init")
        private Builder() {
            this.index = -1;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDataType(final DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setIndex(int index) {
            Assert.thatUnchecked(index >= 0);
            this.index = index;
            return this;
        }

        public RecordLayerColumn build() {
            return new RecordLayerColumn(name, dataType, index);
        }
    }

    public static RecordLayerColumn from(final DataType.StructType.Field field) {
        return new RecordLayerColumn(field.getName(), field.getType(), field.getIndex());
    }

    public static RecordLayerColumn.Builder newBuilder() {
        return new Builder();
    }
}
