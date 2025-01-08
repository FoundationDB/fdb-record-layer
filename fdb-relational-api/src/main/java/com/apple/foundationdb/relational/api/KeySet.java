/*
 * KeySet.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class KeySet {
    public static final KeySet EMPTY = new KeySet() {
        @Override
        public Map<String, Object> toMap() {
            return Collections.emptyMap();
        }

        @Override
        public KeySet setKeyColumn(String columnName, Object value) throws SQLException {
            throw new SQLException("The Empty Keyset cannot be modified", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());

        }
    };

    @SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
    private Map<String, Object> keySet;

    public Map<String, Object> toMap() {
        if (keySet == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(keySet);
    }

    /**
     * Sets the map entry (columnName.UPPER, value).
     * @param columnName the name of the column
     * @param value the value object to insert in the KeySet
     * @return the constructed key set that was inserted in the map
     * @throws SQLException Unsupported operation if the KeySet is immutable
     */
    public KeySet setKeyColumn(String columnName, Object value) throws SQLException {
        if (keySet == null) {
            keySet = new HashMap<>();
        }
        //make sure that all keys are in UPPER case to make it easier to use
        keySet.put(columnName, value);
        return this;
    }

    public KeySet setKeyColumns(Map<String, Object> keyMap) {
        if (keySet == null) {
            keySet = new HashMap<>();
        }
        //make sure that all keys are in UPPER case to make it easier to use
        keySet.putAll(keyMap);
        return this;
    }

    @Override
    public String toString() {
        return "KeySet{" +
                keySet +
                '}';
    }
}
