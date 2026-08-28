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
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableMap;

import org.jspecify.annotations.Nullable;
import java.util.Map;
import java.util.Objects;

import static com.apple.foundationdb.record.RecordMetaDataProto.Predicate;

@API(API.Status.EXPERIMENTAL)
public final class RecordLayerIndex implements Index  {

    private final String tableName;

    private final String tableStorageName;

    private final String indexType;

    private final String name;

    private final KeyExpression keyExpression;

    private final Map<String, String> options;

    @Nullable
    private final Predicate predicate;

    private RecordLayerIndex(final String tableName,
                             final String tableStorageName,
                             final String indexType,
                             final String name,
                             final KeyExpression keyExpression,
                             @Nullable final Predicate predicate,
                             final Map<String, String> options) {
        this.tableName = tableName;
        this.tableStorageName = tableStorageName;
        this.indexType = indexType;
        this.name = name;
        this.keyExpression = keyExpression;
        this.predicate = predicate;
        this.options = ImmutableMap.copyOf(options);
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public String getTableStorageName() {
        return tableStorageName;
    }

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
    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public String getName() {
        return name;
    }

    public KeyExpression getKeyExpression() {
        return keyExpression;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public static RecordLayerIndex from(final String tableName, String tableStorageName, final com.apple.foundationdb.record.metadata.Index index) {
        return newBuilder()
                .setName(index.getName())
                .setIndexType(index.getType())
                .setTableName(tableName)
                .setTableStorageName(tableStorageName)
                .setKeyExpression(index.getRootExpression())
                .setPredicate(index.hasPredicate() ? index.getPredicate().toProto() : null)
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
        private ImmutableMap.@Nullable Builder<String, String> optionsBuilder;

        @Nullable
        private Predicate predicate;

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setTableStorageName(String tableStorageName) {
            this.tableStorageName = tableStorageName;
            return this;
        }

        public Builder setTableType(Type.Record tableType) {
            return setTableName(tableType.getName())
                    .setTableStorageName(tableType.getStorageName());
        }

        public Builder setIndexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setKeyExpression(KeyExpression keyExpression) {
            this.keyExpression = keyExpression;
            return this;
        }

        public Builder setPredicate(@Nullable final Predicate predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder setUnique(boolean isUnique) {
            return setOption(IndexOptions.UNIQUE_OPTION, isUnique);
        }

        public Builder setOptions(final Map<String, String> options) {
            optionsBuilder = ImmutableMap.builderWithExpectedSize(options.size());
            optionsBuilder.putAll(options);
            return this;
        }

        public Builder addAllOptions(final Map<String, String> options) {
            if (optionsBuilder == null) {
                optionsBuilder = ImmutableMap.builder();
            }
            optionsBuilder.putAll(options);
            return this;
        }

        public Builder setOption(final String optionKey, final String optionValue) {
            if (optionsBuilder == null) {
                optionsBuilder = ImmutableMap.builder();
            }
            optionsBuilder.put(optionKey, optionValue);
            return this;
        }

        public Builder setOption(final String optionKey, int optionValue) {
            return setOption(optionKey, Integer.toString(optionValue));
        }

        public Builder setOption(final String optionKey, boolean optionValue) {
            return setOption(optionKey, Boolean.toString(optionValue));
        }

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

    public static Builder newBuilder() {
        return new Builder();
    }
}
