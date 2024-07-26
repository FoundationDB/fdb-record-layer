/*
 * BunchedMapException.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.util.LoggableException;
import javax.annotation.Nonnull;

/**
 * Exception class that can be thrown by a {@link BunchedMap}. Exceptions of this class
 * might be thrown if some internal invariant of the <code>BunchedMap</code> class
 * has been broken (similar to an assertion error). Errors from serializing or deserializing
 * keys and values should throw a {@link BunchedSerializationException}, which is a subclass
 * of this exception, rather than instances of the base exception.
 *
 * @see BunchedMap
 * @see BunchedSerializationException
 */
@SuppressWarnings("serial")
@API(API.Status.EXPERIMENTAL)
public class BunchedMapException extends LoggableException {
    /**
     * Create a new exception with a static message.
     * @param message error message
     */
    public BunchedMapException(@Nonnull String message) {
        super(message);
    }

    /**
     * Create a new exception with a static message and cause.
     *
     * @param message error message
     * @param cause cause
     */
    public BunchedMapException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    @Nonnull
    @Override
    public BunchedMapException addLogInfo(@Nonnull String description, Object object) {
        super.addLogInfo(description, object);
        return this;
    }

    @Nonnull
    @Override
    public BunchedMapException addLogInfo(@Nonnull Object... keyValue) {
        super.addLogInfo(keyValue);
        return this;
    }
}
