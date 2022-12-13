/*
 * RecordLayerIndex.java
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

import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;

public final class RecordLayerIndex implements Index  {

    @Nonnull
    private final String tableName;

    private final String indexType;

    @Nonnull
    private final String name;

    @Nonnull
    private final KeyExpression keyExpression;

    private final boolean isUnique;

    private RecordLayerIndex(@Nonnull final String tableName,
                             @Nonnull final String indexType,
                             @Nonnull final String name,
                             @Nonnull final KeyExpression keyExpression,
                             boolean isUnique) {
        this.tableName = tableName;
        this.indexType = indexType;
        this.name = name;
        this.keyExpression = keyExpression;
        this.isUnique = isUnique;
    }

    @Nonnull
    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public @Nonnull String getIndexType() {
        return indexType;
    }

    @Override
    public boolean isUnique() {
        return isUnique;
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
    public static RecordLayerIndex from(@Nonnull final String tableName, @Nonnull final com.apple.foundationdb.record.metadata.Index index) {
        return newBuilder().setName(index.getName())
                .setIndexType(index.getType())
                .setTableName(tableName)
                .setKeyExpression(index.getRootExpression())
                .setUnique(index.getBooleanOption(IndexOptions.UNIQUE_OPTION, false))
                .build();
    }

    public static class Builder {
        private String tableName;
        private String indexType;
        private String name;
        private KeyExpression keyExpression;

        private boolean isUnique;

        @Nonnull
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
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

        public Builder setUnique(boolean isUnique) {
            this.isUnique = isUnique;
            return this;
        }

        @Nonnull
        public RecordLayerIndex build() {
            Assert.notNullUnchecked(name, "index name is not set");
            Assert.notNullUnchecked(tableName, "table name is not set");
            Assert.notNullUnchecked(indexType, "index type is not set");
            Assert.notNullUnchecked(keyExpression, "index key expression is not set");
            return new RecordLayerIndex(tableName, indexType, name, keyExpression, isUnique);
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }
}
