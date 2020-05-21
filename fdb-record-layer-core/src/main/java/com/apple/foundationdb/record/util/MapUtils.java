/*
 * MapUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import java.util.Map;
import java.util.function.Function;

/**
 * Utility functions for manipulating maps.
 */
public class MapUtils {

    private MapUtils() {
    }

    /**
     * In older versions of java {@code ConcurrentHashMap.computeIfAbsent()} has an issue where threads can contend
     * on reads even when the value is present.  This method provides the same behavior as {@code computeIfAbsent}
     * but avoids such contention.
     *
     * @param map the map to use for the operation
     * @param key the key to look up
     * @param supplier a supplier to be used to generate a new value if one is not already present
     * @param <K> the key type
     * @param <V> the value type
     * @return the existing value from the map or the newly added value if no value was previously present
     */
    public static <K, V> V computeIfAbsent(Map<K, V> map, K key, Function<K, V> supplier) {
        V value = map.get(key);
        if (value == null) {
            value = map.computeIfAbsent(key, supplier);
        }
        return value;
    }
}
