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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class KeySet {
    public static final KeySet EMPTY = new KeySet() {
        @Override
        public Map<String, Object> toMap() {
            return Collections.emptyMap();
        }

        @Override
        public KeySet setKeyColumn(String columnName, Object value) throws RelationalException {
            throw new RelationalException("The Empty Keyset cannot be modified", ErrorCode.UNSUPPORTED_OPERATION);
        }
    };

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
     * @throws RelationalException Unsupported operation if the KeySet is immutable
     */
    public KeySet setKeyColumn(String columnName, Object value) throws RelationalException {
        if (keySet == null) {
            keySet = new HashMap<>();
        }
        //make sure that all keys are in UPPER case to make it easier to use
        keySet.put(columnName.toUpperCase(Locale.ROOT), value);
        return this;
    }

    public KeySet setKeyColumns(Map<String, Object> keyMap) throws RelationalException {
        if (keySet == null) {
            keySet = new HashMap<>();
        }
        //make sure that all keys are in UPPER case to make it easier to use
        for (Map.Entry<String, Object> keyEntry : keyMap.entrySet()) {
            keySet.put(keyEntry.getKey().toUpperCase(Locale.ROOT), keyEntry.getValue());
        }
        return this;
    }

    @Override
    public String toString() {
        return "KeySet{" +
                keySet +
                '}';
    }
}
