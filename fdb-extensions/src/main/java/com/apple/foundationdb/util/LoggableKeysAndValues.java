/*
 * LoggableKeysAndValues.java
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

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Associates loggable information with an object as a map.
 * Record layer log messages are (relatively) well formed. They are
 * comprised of a static "title" of the message along with a set of
 * keys and values that provide context about the "title" of the message.
 * For example a "File not found" log entry may include keys and values
 * like filename="foo" and directory="/bar". This makes the logs easy
 * to generally search for all such errors, later extracting the set of
 * files and directories that couldn't be found. This interface defines
 * the methods that all objects that wish to provide such logging details
 * must implement.
 *
 * @typeparam T type of object to associate loggable information with
 */
interface LoggableKeysAndValues<T extends LoggableKeysAndValues<T>> {

    /**
     * Get the log information associated with object as a map.
     *
     * @return a single map with all log information
     */
    @Nonnull
    Map<String, Object> getLogInfo();

    /**
     * Add a key/value pair to the log information. This will use the description
     * given as the key and the object provided as the value.
     *
     * @param description description of the log info pair
     * @param object value of the log info pair
     * @return this <code>LoggableException</code>
     */
    @Nonnull
    T addLogInfo(@Nonnull String description, Object object);

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
     * @return this <code>T</code>
     * @throws IllegalArgumentException if <code>keyValue</code> has odd length
     */
    @Nonnull
    T addLogInfo(@Nonnull Object ... keyValue);

    /**
     * Export the log information to a flattened array. This will flatten the map that would
     * be returned by {@link #getLogInfo()} into an array where every even element is a key
     * within the map and every odd element is the value associated with the key before it. So,
     * for example, <code>{"k0:"v0", "k1":"v1"}</code> would be flattened into
     * <code>["k0", "v0", "k1", "v1"]</code>. Note that this is the same format that is
     * accepted by {@link #addLogInfo(Object...)}.
     *
     * @return a flattened map of key-value pairs
     */
    @Nonnull
    Object[] exportLogInfo();

}
