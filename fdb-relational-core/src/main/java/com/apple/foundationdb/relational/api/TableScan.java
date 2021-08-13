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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a direct scan against a table, index, or heap.
 */
public class TableScan {
    private final String tableName;
    private final Map<String, Object> startKey;
    private final Map<String, Object> endKey;

    public TableScan(String tableName, Map<String, Object> startKey, Map<String, Object> endKey) {
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
        return startKey;
    }

    public Map<String, Object> getEndKey() {
        return endKey;
    }

    public static class Builder {
        private String tableName;
        private Map<String, Object> startKey;
        private Map<String, Object> endKey;

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setStartKey(String keyColumn, Object value) {
            if (this.startKey == null) {
                this.startKey = new HashMap<>();
            }
            this.startKey.put(keyColumn, value);
            return this;
        }

        public Builder setEndKey(String keyColumn, Object value) {
            if (this.endKey == null) {
                this.endKey = new HashMap<>();
            }
            this.endKey.put(keyColumn, value);
            return this;
        }

        public TableScan build() {
            Preconditions.checkNotNull(this.tableName, "Cannot create a scan without a table name");

            Map<String, Object> sk = startKey != null ? Collections.unmodifiableMap(startKey) : Collections.emptyMap();
            Map<String, Object> ek = endKey != null ? Collections.unmodifiableMap(endKey) : Collections.emptyMap();

            return new TableScan(tableName, sk, ek);
        }
    }
}
