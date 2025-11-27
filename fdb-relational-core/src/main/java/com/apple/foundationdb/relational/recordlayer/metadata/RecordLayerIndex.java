/*
 * RecordLayerIndex.java
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public final class RecordLayerIndex implements Index  {

    @Nonnull
    private final String tableName;

    @Nonnull
    private final String tableStorageName;

    private final String indexType;

    @Nonnull
    private final String name;

    @Nonnull
    private final KeyExpression keyExpression;

    @Nonnull
    private final Map<String, String> options;

    @Nullable
    private final RecordMetaDataProto.Predicate predicate;

    private RecordLayerIndex(@Nonnull final String tableName,
                             @Nonnull final String tableStorageName,
                             @Nonnull final String indexType,
                             @Nonnull final String name,
                             @Nonnull final KeyExpression keyExpression,
                             @Nullable final RecordMetaDataProto.Predicate predicate,
                             @Nonnull final Map<String, String> options) {
        this.tableName = tableName;
        this.tableStorageName = tableStorageName;
        this.indexType = indexType;
        this.name = name;
        this.keyExpression = keyExpression;
        this.predicate = predicate;
        this.options = ImmutableMap.copyOf(options);
    }

    @Nonnull
    @Override
    public String getTableName() {
        return tableName;
    }

    @Nonnull
    public String getTableStorageName() {
        return tableStorageName;
    }

    @Nonnull
    @Override
    public String getIndexType() {
        return indexType;
    }

    @Override
    public boolean isUnique() {
        @Nullable String uniqueOption = options.get(IndexOptions.UNIQUE_OPTION);
        return Boolean.parseBoolean(uniqueOption);
    }

    @Override
    public boolean isSparse() {
        return predicate != null;
    }

    @Nullable
    public RecordMetaDataProto.Predicate getPredicate() {
        return predicate;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    public KeyExpression getKeyExpression() {
        return keyExpression;
    }

    @Nonnull
    public Map<String, String> getOptions() {
        return options;
    }

    @Nonnull
    public Builder toBuilder() {
        return newBuilder().setName(getName())
                .setIndexType(getIndexType())
                .setTableName(getTableName())
                .setUnique(isUnique())
                .setKeyExpression(getKeyExpression())
                .setPredicate(getPredicate())
                .setOptions(getOptions());
    }

    @Nonnull
    public static RecordLayerIndex from(@Nonnull final String tableName, @Nonnull String tableStorageName, @Nonnull final com.apple.foundationdb.record.metadata.Index index) {
        final var indexProto = index.toProto();
        return newBuilder().setName(index.getName())
                .setIndexType(index.getType())
                .setTableName(tableName)
                .setTableStorageName(tableStorageName)
                .setKeyExpression(index.getRootExpression())
                .setPredicate(indexProto.hasPredicate() ? indexProto.getPredicate() : null)
                .setOptions(index.getOptions())
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordLayerIndex that = (RecordLayerIndex) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(indexType, that.indexType) &&
                Objects.equals(name, that.name) &&
                Objects.equals(keyExpression, that.keyExpression) &&
                Objects.equals(predicate, that.predicate) &&
                Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, indexType, name, keyExpression, options, predicate);
    }

    public static class Builder {
        private String tableName;
        private String tableStorageName;
        private String indexType;
        private String name;
        private KeyExpression keyExpression;
        @Nullable
        private ImmutableMap.Builder<String, String> optionsBuilder;

        @Nullable
        private RecordMetaDataProto.Predicate predicate;

        @Nonnull
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Nonnull
        public Builder setTableStorageName(String tableStorageName) {
            this.tableStorageName = tableStorageName;
            return this;
        }

        @Nonnull
        public Builder setTableType(@Nonnull Type.Record tableType) {
            return setTableName(tableType.getName())
                    .setTableStorageName(tableType.getStorageName());
        }

        @Nonnull
        public Builder setIndexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        @Nonnull
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setKeyExpression(KeyExpression keyExpression) {
            this.keyExpression = keyExpression;
            return this;
        }

        @Nonnull
        public Builder setPredicate(@Nullable final RecordMetaDataProto.Predicate predicate) {
            this.predicate = predicate;
            return this;
        }

        @Nonnull
        public Builder setUnique(boolean isUnique) {
            return setOption(IndexOptions.UNIQUE_OPTION, isUnique);
        }

        @Nonnull
        public Builder setOptions(@Nonnull final Map<String, String> options) {
            optionsBuilder = ImmutableMap.builderWithExpectedSize(options.size());
            optionsBuilder.putAll(options);
            return this;
        }

        @Nonnull
        public Builder addAllOptions(@Nonnull final Map<String, String> options) {
            if (optionsBuilder == null) {
                optionsBuilder = ImmutableMap.builder();
            }
            optionsBuilder.putAll(options);
            return this;
        }

        @Nonnull
        public Builder setOption(@Nonnull final String optionKey, @Nonnull final String optionValue) {
            if (optionsBuilder == null) {
                optionsBuilder = ImmutableMap.builder();
            }
            optionsBuilder.put(optionKey, optionValue);
            return this;
        }

        @Nonnull
        public Builder setOption(@Nonnull final String optionKey, int optionValue) {
            return setOption(optionKey, Integer.toString(optionValue));
        }

        @Nonnull
        public Builder setOption(@Nonnull final String optionKey, boolean optionValue) {
            return setOption(optionKey, Boolean.toString(optionValue));
        }

        @Nonnull
        public RecordLayerIndex build() {
            Assert.notNullUnchecked(name, "index name is not set");
            Assert.notNullUnchecked(tableName, "table name is not set");
            if (tableStorageName == null) {
                tableStorageName = ProtoUtils.toProtoBufCompliantName(tableName);
            }
            Assert.notNullUnchecked(indexType, "index type is not set");
            Assert.notNullUnchecked(keyExpression, "index key expression is not set");
            return new RecordLayerIndex(tableName, tableStorageName, indexType, name, keyExpression, predicate,
                    optionsBuilder == null ? ImmutableMap.of() : optionsBuilder.build());
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }
}
