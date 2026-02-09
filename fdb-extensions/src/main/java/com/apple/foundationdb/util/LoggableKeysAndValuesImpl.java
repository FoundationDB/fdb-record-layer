/*
 * LoggableKeysAndValuesImpl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.util;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a default implementation of {@link LoggableKeysAndValues}.
 */
@SuppressWarnings("serial")
@API(API.Status.UNSTABLE)
public final class LoggableKeysAndValuesImpl implements LoggableKeysAndValues<LoggableKeysAndValuesImpl> {
    private static final Object[] EMPTY_LOG_INFO = new Object[0];
    @Nullable private Map<String, Object> logInfo;

    /**
     * Create an instance with the given message and a sequence of key-value pairs.
     * This will throw an {@link IllegalArgumentException} if <code>keyValues</code>
     * contains an odd number of elements.
     *
     * @param keyValues list
     * @see #addLogInfo(Object...)
     */
    public LoggableKeysAndValuesImpl(@Nullable Object ... keyValues) {
        if (keyValues != null) {
            addLogInfo(keyValues);
        }
    }

    /**
     * Get the log information associated with this exception as a map.
     *
     * @return a single map with all log information
     */
    @Nonnull
    @Override
    public Map<String, Object> getLogInfo() {
        if (logInfo == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(logInfo);
    }

    /**
     * Add a key/value pair to the log information. This will use the description
     * given as the key and the object provided as the value.
     *
     * @param description description of the log info pair
     * @param object value of the log info pair
     * @return this <code>LoggableException</code>
     */
    @Nonnull
    @Override
    public LoggableKeysAndValuesImpl addLogInfo(@Nonnull String description, Object object) {
        if (logInfo == null) {
            logInfo = new HashMap<>();
        }
        logInfo.put(description, object);
        return this;
    }

    /**
     * Add a list of key/value pairs to the log information. This will treat the
     * list of items as pairs to be added with every even element being a key
     * and every odd element being a value (associated with the even key preceding
     * it). So, for example, <code>["k0", "v0", "k1", "v1"]</code> will add two
     * pairs to the log info, one with key <code>"k0"</code> and value <code>"v0"</code>
     * and one with key <code>"k1"</code> and value <code>"v1"</code>. Note that
     * this is the same format that is exported by {@link #exportLogInfo()}.
     *
     * @param keyValue flattened map of key-value pairs
     * @return this <code>LoggableException</code>
     * @throws IllegalArgumentException if <code>keyValue</code> has odd length
     */
    @Nonnull
    @Override
    public LoggableKeysAndValuesImpl addLogInfo(@Nonnull Object ... keyValue) {
        if ((keyValue.length % 2) != 0) {
            throw new IllegalArgumentException("Unbalanced key/value logging info");
        }

        for (int i = 0; i < keyValue.length; i += 2) {
            addLogInfo(String.valueOf(keyValue[i]), keyValue[i + 1]);
        }
        return this;
    }

    /**
     * Export the log information to a flattened array. This will flatten the map that would
     * be returned by {@link #getLogInfo()} into an array where every even element is a key
     * within the map and every odd element is the value associated with the key before it. So,
     * for example, <code>{"k0:"v0", "k1":"v1"}</code> would be flattened into
     * <code>["k0", "v0", "k1", "v1"]</code>.
     *
     * @return a flattened map of key-value pairs
     */
    @Nonnull
    @Override
    public Object[] exportLogInfo() {
        if (logInfo == null) {
            return EMPTY_LOG_INFO;
        }
        Object[] exportedInfo = new Object[2 * logInfo.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : logInfo.entrySet()) {
            exportedInfo[i] = entry.getKey();
            exportedInfo[i + 1] = entry.getValue();
            i += 2;
        }
        return exportedInfo;
    }
}
