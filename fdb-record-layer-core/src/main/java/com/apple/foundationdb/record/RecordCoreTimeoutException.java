/*
 * RecordCoreTimeoutException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This exception extends {@link RecordCoreException} and is specifically used to indicate
 * timeout-related failures.
 */
@API(API.Status.UNSTABLE)
@SuppressWarnings("serial")
public class RecordCoreTimeoutException extends RecordCoreException {
    public RecordCoreTimeoutException(@Nonnull String msg, @Nullable Object ... keyValues) {
        super(msg, keyValues);
    }

    public RecordCoreTimeoutException(Throwable cause) {
        super(cause);
    }

    public RecordCoreTimeoutException(@Nonnull String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    public RecordCoreTimeoutException(@Nonnull String msg) {
        super(msg);
    }

    protected RecordCoreTimeoutException(String message, Throwable cause,
                                         boolean enableSuppression,
                                         boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
