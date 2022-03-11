/*
 * SemanticException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;

/**
 * Semantic exceptions that could occur e.g. to illegal type conversions, ... etc.
 */
public class SemanticException extends RecordCoreException {
    private static final long serialVersionUID = 101714053557545076L;

    public SemanticException(final String message) {
        super(message);
    }

    public SemanticException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public static void check(final boolean condition, @Nonnull final String message) {
        if (!condition) {
            throw new SemanticException(message);
        }
    }
}
