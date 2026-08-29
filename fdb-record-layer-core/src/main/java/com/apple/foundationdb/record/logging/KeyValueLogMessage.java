/*
 * KeyValueLogMessage.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.logging;

import com.apple.foundationdb.annotation.API;

import org.jspecify.annotations.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A formatter for log messages.
 *
 * A {@code KeyValueLogMessage} has an associated set of key-value pairs, which are output after the static portion of the message in {@code key=value} form.
 */
@API(API.Status.UNSTABLE)
public class KeyValueLogMessage {
    private final long timestamp;
    private final String staticMessage;

    private final Map<String, String> keyValueMap;

    public static String of(final String staticMessage, @Nullable final Object... keysAndValues) {
        return new KeyValueLogMessage(staticMessage, mapFromKeysAndValues(keysAndValues)).toString();
    }

    public static KeyValueLogMessage build(final String staticMessage, @Nullable final Object... keysAndValues) {
        return new KeyValueLogMessage(staticMessage, mapFromKeysAndValues(keysAndValues));
    }

    private static Map<String, String> mapFromKeysAndValues(@Nullable Object[] keysAndValues) {
        Map<String, String> keyValueMap = new TreeMap<>();
        if (keysAndValues != null) {
            for (int i = 0; i < (keysAndValues.length / 2) * 2; i += 2) {
                addKeyValueImpl(keyValueMap, keysAndValues[i], keysAndValues[i + 1]);
            }
            if (keysAndValues.length % 2 == 1) {
                throw new IllegalArgumentException("keys and values don't match");
            }
        }
        return keyValueMap;
    }

    private KeyValueLogMessage(final String staticMessage, Map<String, String> keyValueMap) {
        this.staticMessage = staticMessage;
        this.timestamp = System.currentTimeMillis();
        this.keyValueMap = keyValueMap;
    }

    private static void addKeyValueImpl(Map<String, String> keyValueMap, @Nullable final Object key, @Nullable Object value) {
        if (key == null) {
            throw new IllegalArgumentException("null key passed to KeyValueLogMessage"); // better to throw an exception rather than silently swallow the key
        }
        keyValueMap.put(sanitizeKey(key.toString()), sanitizeValue(Optional.ofNullable(value).orElse("null").toString()));
    }

    private void addKeyValueImpl(@Nullable final Object key, @Nullable Object value) {
        addKeyValueImpl(keyValueMap, key, value);
    }

    public KeyValueLogMessage addKeysAndValues(final Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            addKeyValueImpl(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public KeyValueLogMessage addKeysAndValues(final List<Object> keyValues) {
        for (int u = 0; u < (keyValues.size() / 2) * 2; u += 2) {
            addKeyValueImpl(keyValues.get(u), keyValues.get(u + 1));
        }
        return this;
    }

    private static String sanitizeValue(final String value) {
        return value.replace("\"", "'");
    }

    private static String sanitizeKey(final String key) {
        return key.replace("=", "");
    }

    @Override
    public String toString() {
        return getMessageWithKeys();
    }

    public String getMessageWithKeys() {
        final StringBuilder sb = new StringBuilder( keyValueMap.size() * 30);
        sb.append(staticMessage);
        for (Map.Entry<String, String> entry: keyValueMap.entrySet()) {
            sb.append(" ");
            sb.append(entry.getKey());
            sb.append("=\"");
            sb.append(entry.getValue());
            sb.append("\"");
        }
        return sb.toString();
    }

    public Object[] getValues() {
        return keyValueMap.values().toArray();
    }

    public Object[] getValuesWithThrowable(@Nullable final Throwable t) {
        final Object[] ret = keyValueMap.values().toArray(new Object[keyValueMap.size() + 1]);
        ret[ret.length - 1] = t;
        return ret;
    }

    public KeyValueLogMessage addKeyAndValue(final Object key, @Nullable final Object value) {
        addKeyValueImpl(key, value);
        return this;
    }

    public String getStaticMessage() {
        return keyValueMap.get(LogMessageKeys.TITLE.toString());
    }

    public void setStaticMessage(final String staticMessage) {
        keyValueMap.put(LogMessageKeys.TITLE.toString(), staticMessage);
    }

    public long getTimeStamp() {
        return timestamp;
    }

    public Map<String, String> getKeyValueMap() {
        return Collections.unmodifiableMap(keyValueMap);
    }
}

