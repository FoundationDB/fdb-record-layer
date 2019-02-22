/*
 * IllegalContinuationAccessChecker.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;

/**
 * Check that {@link com.apple.foundationdb.record.RecordCursor#getContinuation} is only called when allowed.
 *
 * {@code getContinuation} is only legal after {@code hasNext} has returned {@code false} or after {@code next}.
 *
 * The check can be enabled / disabled globally.
 */
@API(API.Status.EXPERIMENTAL)
public class IllegalContinuationAccessChecker {
    private static boolean shouldCheckContinuationAccess = true;

    public static void check(boolean mayGetContinuation) {
        if (shouldCheckContinuationAccess && !mayGetContinuation) {
            throw new RecordCoreException("illegal continuation access");
        }
    }

    public static void setShouldCheckContinuationAccess(boolean shouldCheckContinuationAccess) {
        IllegalContinuationAccessChecker.shouldCheckContinuationAccess = shouldCheckContinuationAccess;
    }

    @SuppressWarnings("PMD.BooleanGetMethodName")
    public static boolean getShouldCheckContinuationAccess() {
        return shouldCheckContinuationAccess;
    }

    private IllegalContinuationAccessChecker() {}
}
