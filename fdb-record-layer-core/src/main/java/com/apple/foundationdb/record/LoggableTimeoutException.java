/*
 * LoggableTimeoutException.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.util.LoggableKeysAndValues;
import com.apple.foundationdb.util.LoggableKeysAndValuesImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Subclass of TimeoutException with support for adding logging info in the form of keys and values. This enables
 * TimeoutException's
 * to provide context-specific details that can be logged in a way that better supports troubleshooting later.
 */
@SuppressWarnings("serial")
@API(API.Status.UNSTABLE)
public final class LoggableTimeoutException extends TimeoutException implements LoggableKeysAndValues<LoggableTimeoutException> {
    @Nonnull
    private final LoggableKeysAndValuesImpl loggableKeysAndValuesImpl = new LoggableKeysAndValuesImpl();


    /**
     * Create an exception with the given sequence of key/value pairs.
     * This will throw an {@link IllegalArgumentException} if <code>keyValues</code>
     * contains an odd number of elements.
     *
     * @param cause root cause of the timeout exception
     * @param keyValues loggable keys and values
     *
     * @see #addLogInfo(Object...)
     */
    public LoggableTimeoutException(@Nonnull Throwable cause, @Nullable Object... keyValues) {
        super();
        super.initCause(cause);
        this.loggableKeysAndValuesImpl.addLogInfo(keyValues);
    }

    @Nonnull
    @Override
    public Map<String, Object> getLogInfo() {
        return loggableKeysAndValuesImpl.getLogInfo();
    }

    @Nonnull
    @Override
    public LoggableTimeoutException addLogInfo(@Nonnull String description, Object object) {
        loggableKeysAndValuesImpl.addLogInfo(description, object);
        return this;
    }

    @Nonnull
    @Override
    public LoggableTimeoutException addLogInfo(@Nonnull Object... keyValue) {
        loggableKeysAndValuesImpl.addLogInfo(keyValue);
        return this;
    }
    
    @Nonnull
    @Override
    public Object[] exportLogInfo() {
        return loggableKeysAndValuesImpl.exportLogInfo();
    }
}
