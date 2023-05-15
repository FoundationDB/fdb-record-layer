/*
 * KeyValueLoggingMessage.java
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

package com.apple.foundationdb.relational.recordlayer.util;

import org.apache.logging.log4j.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A Log4j Message that appends key-value pair data to the end. Useful for formatting things for splunk
 * etc.
 */
public class KeyValueLoggingMessage implements Message {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> keyValueMap = new TreeMap<>();

    public KeyValueLoggingMessage(@Nonnull final String staticMessage) {
        keyValueMap.put("title", staticMessage.trim());
    }

    @Override
    public String getFormattedMessage() {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            builder.append(entry.getKey())
                    .append("=")
                    .append(entry.getValue())
                    .append(++count < keyValueMap.size() ? ", " : "");
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return this.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return "";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }

    public static KeyValueLoggingMessage create(String staticMessage) {
        return new KeyValueLoggingMessage(staticMessage);
    }

    public KeyValueLoggingMessage append(@Nonnull final Object key, @Nullable Object value) {
        keyValueMap.put(sanitizeKey(key.toString()), sanitizeValue(Optional.ofNullable(value).orElse("null").toString()));
        return this;
    }

    public static String sanitizeKey(@Nonnull final String key) {
        return key.replace("=", "");
    }

    public static String sanitizeValue(@Nonnull final String value) {
        return value.replace("\"", "'");
    }
}
