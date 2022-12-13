/*
 * RecordLayerColumn.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.relational.api.metadata.Column;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;

public class RecordLayerColumn implements Column {
    @Nonnull
    private final String name;

    @Nonnull
    private final DataType dataType;

    RecordLayerColumn(@Nonnull String name, @Nonnull DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    @Override
    public DataType getDatatype() {
        return dataType;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    public DataType getDataType() {
        return dataType;
    }

    public static final class Builder {
        private String name;
        private DataType dataType;

        private Builder() {
            // no-op
        }

        @Nonnull
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setDataType(@Nonnull final DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public RecordLayerColumn build() {
            return new RecordLayerColumn(name, dataType);
        }
    }

    @Nonnull
    public static RecordLayerColumn from(@Nonnull final DataType.StructType.Field field) {
        return new RecordLayerColumn(field.getName(), field.getType());
    }

    @Nonnull
    public static RecordLayerColumn.Builder newBuilder() {
        return new Builder();
    }
}
