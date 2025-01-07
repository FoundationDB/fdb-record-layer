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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.google.common.annotations.VisibleForTesting;

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
@API(API.Status.UNSTABLE)
public class CompletionExceptionLogHelper {

    private static boolean addSuppressed = true;
    private static int maxSuppressedCount = Integer.MAX_VALUE;

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
     * Change the maximum number of suppressed exceptions to add to any given exception, if this maximum has not already
     * been set. Return whether or not the maximum was changed.
     *
     * This method only changes behavior when {@link #addSuppressed} is {@code true}.
     * @param count the new maximum count
     * @return {@code true} if the count was changed and {@code false} if it was not
     */
    public static synchronized boolean setMaxSuppressedCountIfNotSet(int count) {
        if (count < 0) {
            throw new RecordCoreArgumentException("tried to set max suppressed count to a negative value");
        }
        if (maxSuppressedCount == Integer.MAX_VALUE && count != Integer.MAX_VALUE) {
            maxSuppressedCount = count;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Change the maximum number of suppressed exceptions to add to any given exception, even if the maximum has already
     * been set. This should be done with extreme care: {@link #asCauseThrowable(Throwable)} may misbehave if it
     * handles exceptions before and after this method is called with the same {@code cause}. It should ONLY be used
     * for testing without restarting the JVM.
     *
     * @param count the new maximum count
     */
    @VisibleForTesting
    @API(API.Status.INTERNAL)
    public static synchronized void forceSetMaxSuppressedCountForTesting(int count) {
        if (count < 0) {
            throw new RecordCoreArgumentException("tried to set max suppressed count to a negative value");
        }
        maxSuppressedCount = count;
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
     * However, if the given exception's cause already has the maximum number of suppressed exceptions specified by
     * {@link #setMaxSuppressedCountIfNotSet(int)}, then the given exception is not added to the suppressed chain and
     * the exception counting the number of ignored exceptions is incremented instead.
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
            synchronized (cause) {
                Throwable[] suppressedExceptions = cause.getSuppressed();
                if (suppressedExceptions.length < maxSuppressedCount) {
                    cause.addSuppressed(ex);
                } else {
                    IgnoredSuppressedExceptionCount suppressedCount;
                    int lastIndex = suppressedExceptions.length - 1;
                    if (lastIndex >= 0 && suppressedExceptions[lastIndex] instanceof IgnoredSuppressedExceptionCount) {
                        suppressedCount = (IgnoredSuppressedExceptionCount) suppressedExceptions[lastIndex];
                    } else {
                        suppressedCount = new IgnoredSuppressedExceptionCount();
                        cause.addSuppressed(suppressedCount);
                    }
                    suppressedCount.incrementCount();
                }
            }
        }
        return cause;
    }

    /**
     * A special "exception" to record the number of suppressed exceptions that were not recorded due to the
     * {@link #maxSuppressedCount}.
     */
    public static class IgnoredSuppressedExceptionCount extends Exception {
        private static final long serialVersionUID = 1L;

        private int count;

        private IgnoredSuppressedExceptionCount() {
            // message is the empty string becuase we override getMessage().
            // cause is null
            // enable suppression
            // disable stack trace collection, since we don't want it
            super("", null, true, false);
        }

        public synchronized void incrementCount() {
            count++;
        }

        @Override
        public String getMessage() {
            return "Ignoring " + count + " suppressed exceptions.";
        }

        public int getCount() {
            return count;
        }
    }

}
