/*
 * LogMessageKeys.java
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

package com.apple.foundationdb.util;

import com.apple.foundationdb.annotation.API;

import java.util.Locale;

/**
 * Common {@link LoggableException} keys logged by the FoundationDB extensions library.
 * In general, we try to consolidate all of the keys for this library, so that it's easy to check for collisions, ensure consistency, etc.
 */
@API(API.Status.UNSTABLE)
public enum LogMessageKeys {
    SUBSPACE,

    // synchronized session
    SESSION_ID,
    EXISTING_SESSION,
    EXISTING_SESSION_EXPIRE_TIME,

    FIELD_NAME,
    INDEX_FIELDS,
    ;

    private final String logKey;

    LogMessageKeys() {
        this.logKey = name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return logKey;
    }
}
