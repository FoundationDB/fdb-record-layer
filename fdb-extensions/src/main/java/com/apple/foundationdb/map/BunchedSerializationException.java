/*
 * BunchedSerializationException.java
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
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Exception that can be thrown from a {@link BunchedSerializer} while serializing
 * or deserializing an element. It attempts to include information along
 * with the exception such as what inputs into the serializer caused the exception
 * to be thrown.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("serial")
public class BunchedSerializationException extends BunchedMapException {
    @Nullable
    private byte[] data;
    @Nullable
    private Object value;

    /**
     * Create a new exception with a static message.
     * @param message error message
     */
    public BunchedSerializationException(@Nonnull String message) {
        super(message);
    }

    /**
     * Create a new exception with a static message and cause.
     *
     * @param message error message
     * @param cause cause
     */
    public BunchedSerializationException(@Nonnull String message, @Nonnull Throwable cause) {
        super(message, cause);
    }

    /**
     * Set the data array that triggered this exception. This should
     * be set if the error occurs while deserializing an array in
     * order to preserve the array for diagnostic purposes.
     *
     * @param data raw data array that triggered this exception
     * @return this <code>BunchedSerializationException</code>
     */
    @Nonnull
    public BunchedSerializationException setData(@Nonnull byte[] data) {
        this.data = Arrays.copyOf(data, data.length);
        addLogInfo("data", ByteArrayUtil2.loggable(data));
        return this;
    }

    /**
     * Return the raw bytes that triggered this exception. This will
     * be <code>null</code> if the exception was not triggered
     * while deserializing.
     *
     * @return the data array that triggered the exception
     */
    @Nullable
    public byte[] getData() {
        return (data == null) ? null : Arrays.copyOf(data, data.length);
    }

    /**
     * Set the value that triggered this exception. This should
     * be set if the error occurs while serializing an object
     * to bytes. It should contain the item which triggered the
     * error.
     *
     * @param value the value that triggered the exception
     * @return this <code>BunchedSerializationException</code>
     */
    @Nonnull
    public BunchedSerializationException setValue(@Nonnull Object value) {
        this.value = value;
        addLogInfo("value", value);
        return this;
    }

    /**
     * Return the value that triggered this exception if set.
     * This will be <code>null</code> if the exception was not
     * triggered while serializing or if more than one
     * value might have caused the error.
     *
     * @return the value that triggered the exception
     */
    @Nullable
    public Object getValue() {
        return value;
    }
}
