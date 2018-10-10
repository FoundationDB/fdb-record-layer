/*
 * CompletionExceptionLogHelper.java
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

package com.apple.foundationdb.record.logging;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helper class for helping with logging of {@link java.util.concurrent.CompletionException}s.
 *
 * The {@code CompletionException} has a stack trace that indicates the call to a wait point, such as
 * {@link com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext#asyncToSync}, which may
 * be the only indication of what the caller was actually doing.
 * But the cause of this exception is the one to analyze and wrap
 * in an appropriate subclass of {@link com.apple.foundationdb.record.RecordCoreException}, so that
 * is returned, after remembering the original exception as a suppressed exception.
 */
public class CompletionExceptionLogHelper {

    private static boolean addSuppressed = true;

    private CompletionExceptionLogHelper() {
    }

    /**
     * Change whether {@link Throwable#addSuppressed} is used.
     *
     * When enabled, the exception graph will have a circularity. This can cause problems for some logging frameworks
     * and so may need to be disabled if using one of them.
     * @param addSuppressed {@code true} to use {@code addSuppressed} to give more information in logs
     */
    public static void setAddSuppressed(boolean addSuppressed) {
        CompletionExceptionLogHelper.addSuppressed = addSuppressed;
    }

    /**
     * Return the cause of the given exception and also arrange for the original exception to be in the suppressed chain.
     * @param ex an exception from {@link java.util.concurrent.CompletableFuture#join} or the like
     * @return a throwable suitable for use as the cause of a wrapped exception
     */
    public static Throwable asCause(@Nonnull CompletionException ex) {
        return asCauseThrowable(ex);
    }

    /**
     * Return the cause of the given exception and also arrange for the original exception to be in the suppressed chain.
     * @param ex an exception from {@link java.util.concurrent.CompletableFuture#get} or the like
     * @return a throwable suitable for use as the cause of a wrapped exception
     */
    public static Throwable asCause(@Nonnull ExecutionException ex) {
        return asCauseThrowable(ex);
    }

    private static Throwable asCauseThrowable(@Nonnull Throwable ex) {
        final Throwable cause = ex.getCause();
        if (cause == null) {
            return ex;
        }
        if (addSuppressed) {
            cause.addSuppressed(ex);    // Remember the original.
        }
        return cause;
    }
}
