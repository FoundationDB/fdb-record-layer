/*
 * TableGet.java
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

public class TableGet {
    private final String tableName;
    private final Map<String,Object> key;

    public TableGet(String tableName, Map<String, Object> key) {
        this.tableName = tableName;
        this.key = key;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Object> getKey() {
        return key;
    }


    public static class Builder{
        private String tableName;
        private Map<String,Object> key;

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder keyColumn(String keyColumn, Object value) {
            if(this.key ==null){
                this.key = new HashMap<>();
            }
            this.key.put(keyColumn,value);

            return this;
        }

        public TableGet build(){
            Preconditions.checkNotNull(this.tableName,"Cannot create a get without a table name");
            Preconditions.checkArgument(this.key!=null && this.key.size()>0, "Cannot create a get without specifying a key");

            return new TableGet(tableName,Collections.unmodifiableMap(key));
        }
    }
}
