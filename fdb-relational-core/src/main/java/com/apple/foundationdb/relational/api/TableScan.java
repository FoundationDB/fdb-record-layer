/*
 * TableScan.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Representation of a direct scan against a table, index, or heap.
 */
public class TableScan {
    private final String tableName;
    private final KeySet startKey;
    private final KeySet endKey;

    public TableScan(String tableName, KeySet startKey, KeySet endKey) {
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Object> getStartKey() {
        return startKey.toMap();
    }

    public Map<String, Object> getEndKey() {
        return endKey.toMap();
    }

    public static class Builder {
        private String tableName;
        private KeySet startKey;
        private KeySet endKey;

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setStartKey(String keyColumn, Object value) throws RelationalException {
            if (this.startKey == null) {
                this.startKey = new KeySet();
            }
            this.startKey.setKeyColumn(keyColumn, value);
            return this;
        }

        public Builder setEndKey(String keyColumn, Object value) throws RelationalException {
            if (this.endKey == null) {
                this.endKey = new KeySet();
            }
            this.endKey.setKeyColumn(keyColumn, value);
            return this;
        }

        public TableScan build() {
            Preconditions.checkNotNull(this.tableName, "Cannot create a scan without a table name");

            KeySet sk = startKey == null ? KeySet.EMPTY : startKey;
            KeySet ek = endKey == null ? KeySet.EMPTY : endKey;

            return new TableScan(tableName, sk, ek);
        }

        public Builder setStartKeys(KeySet params) {
            this.startKey = params;
            return this;
        }

        public Builder setEndKeys(KeySet params) {
            this.endKey = params;
            return this;
        }
    }
}
