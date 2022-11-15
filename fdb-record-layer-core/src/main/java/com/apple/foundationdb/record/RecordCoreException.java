/*
 * RecordCoreException.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.util.LoggableException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An exception thrown by the core of the Record Layer.
 */
@API(API.Status.STABLE)
@SuppressWarnings("serial")
public class RecordCoreException extends LoggableException {
    public RecordCoreException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }

    public RecordCoreException(Throwable cause) {
        super(cause);
    }

    public RecordCoreException(@Nonnull String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    protected RecordCoreException(String message, Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @Override
    @Nonnull
    public RecordCoreException addLogInfo(@Nonnull String description, Object object) {
        super.addLogInfo(description, object);
        return this;
    }

    @Override
    @Nonnull
    public RecordCoreException addLogInfo(@Nonnull Object ... keyValue) {
        super.addLogInfo(keyValue);
        return this;
    }
}
