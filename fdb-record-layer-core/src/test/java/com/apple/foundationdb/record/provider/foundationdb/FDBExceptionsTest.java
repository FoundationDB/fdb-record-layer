/*
 * FDBExceptionsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreInterruptedException;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.util.LoggableException;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBExceptions}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
class FDBExceptionsTest {
    // Several tests in this class modify the static CompletionExceptionLogHelper. Those tests must be run in serial, so
    // they use the @ResourceLock feature with this named lock to avoid stepping on each other
    @Nonnull
    public static final String COMPLETION_EXCEPTION_HELPER_LOCK = "CompletionExceptionLogHelper";

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    private static final String EXCEPTION_CAUSE_MESSAGE = "the failure cause";
    private static final String PARENT_EXCEPTION_MESSAGE = "something failed asynchronously";

    @Test
    void wrapRuntimeException() {
        Exception cause = createRuntimeException();
        final String methodName = "createRuntimeException";
        testWrappedStackTrace(cause, methodName);
    }

    @Test
    void loggableTimeoutException() {
        CompletableFuture<Void> delayed = new CompletableFuture<>();
        FDBDatabase database = dbExtension.getDatabase();
        database.setAsyncToSyncTimeout(1, TimeUnit.MILLISECONDS);
        LoggableException ex = assertThrows(LoggableException.class, () -> database.asyncToSync(new FDBStoreTimer(), FDBStoreTimer.Waits.WAIT_COMMIT, delayed));
        Map<String, Object> logInfo = ex.getLogInfo();
        assertTrue(logInfo.containsKey(LogMessageKeys.TIME_LIMIT.toString()));
        Assertions.assertEquals((long)(logInfo.get(LogMessageKeys.TIME_LIMIT.toString())), 1L);
        assertTrue(logInfo.containsKey(LogMessageKeys.TIME_UNIT.toString()));
        Assertions.assertEquals(logInfo.get(LogMessageKeys.TIME_UNIT.toString()), TimeUnit.MILLISECONDS);
    }

    @Test
    void wrapCheckedException() {
        Exception cause = createCheckedException();
        final String methodName = "createCheckedException";
        testWrappedStackTrace(cause, methodName);
    }

    @Test
    void wrapInterruptedException() {
        final InterruptedException err = createInterruptedException();
        RuntimeException wrappedErr = FDBExceptions.wrapException(err);
        assertThat(wrappedErr, instanceOf(RecordCoreInterruptedException.class));
        assertThat(wrappedErr.getCause(), sameInstance(err));

        final CompletionException completionErr = new CompletionException(PARENT_EXCEPTION_MESSAGE, err);
        RuntimeException wrappedCompletion = FDBExceptions.wrapException(completionErr);
        assertThat(wrappedCompletion, instanceOf(RecordCoreInterruptedException.class));
        assertThat(wrappedCompletion.getCause(), sameInstance(err));
        assertThat(List.of(err.getSuppressed()), contains(completionErr));

        final ExecutionException executionErr = new ExecutionException(PARENT_EXCEPTION_MESSAGE, err);
        RuntimeException wrappedExecution = FDBExceptions.wrapException(executionErr);
        assertThat(wrappedExecution, instanceOf(RecordCoreInterruptedException.class));
        assertThat(wrappedExecution.getCause(), sameInstance(err));
        assertThat(List.of(err.getSuppressed()), contains(completionErr, executionErr));
    }

    private void testWrappedStackTrace(Exception cause, String methodName) {
        Exception parent = createCompletionException(cause);

        final RuntimeException exception = FDBExceptions.wrapException(parent);
        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        assertThat(sw.toString(), allOf(
                containsString(PARENT_EXCEPTION_MESSAGE),
                containsString(EXCEPTION_CAUSE_MESSAGE),
                containsString(this.getClass().getName() + ".createCompletionException"),
                containsString(this.getClass().getName() + "." + methodName)
        ));
    }

    @ResourceLock(value = COMPLETION_EXCEPTION_HELPER_LOCK, mode = ResourceAccessMode.READ_WRITE)
    @Test
    void tooManySuppressedExceptions() {
        try {
            CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(2);
            final Exception base = createRuntimeException();
            Exception e0 = createCompletionException(base);
            Exception e1 = createCompletionException(base);
            assertEquals(base, FDBExceptions.wrapException(e0));
            assertEquals(base, FDBExceptions.wrapException(e1));

            Throwable[] suppressedExceptions = base.getSuppressed();
            assertEquals(2, suppressedExceptions.length);
            assertThat(Arrays.asList(suppressedExceptions), contains(e0, e1));

            Exception e2 = createCompletionException(base);
            assertEquals(base, FDBExceptions.wrapException(e2));
            suppressedExceptions = base.getSuppressed();
            assertEquals(3, suppressedExceptions.length);
            assertThat(Arrays.asList(suppressedExceptions), hasItems(e0, e1));
            assertThat(Arrays.asList(suppressedExceptions), not(hasItem(e2)));
            Throwable countException = suppressedExceptions[2];
            assertThat(countException, instanceOf(CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount.class));
            assertEquals(1, ((CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount)countException).getCount());

            Exception e3 = createCompletionException(base);
            assertEquals(base, FDBExceptions.wrapException(e3));
            suppressedExceptions = base.getSuppressed();
            assertEquals(3, suppressedExceptions.length);
            assertThat(Arrays.asList(suppressedExceptions), hasItems(e0, e1));
            assertThat(Arrays.asList(suppressedExceptions), not(anyOf(Matchers.<Throwable>hasItem(e2), hasItem(e3))));
            countException = suppressedExceptions[2];
            assertThat(countException, instanceOf(CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount.class));
            assertEquals(2, ((CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount)countException).getCount());
        } finally {
            CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(Integer.MAX_VALUE); //cleanup
        }
    }

    @ResourceLock(value = COMPLETION_EXCEPTION_HELPER_LOCK, mode = ResourceAccessMode.READ_WRITE)
    @Test
    void countSuppressedExceptions() {
        try {
            CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(0);
            final Exception base = createRuntimeException();
            Exception e0 = createCompletionException(base);
            Exception e1 = createCompletionException(base);
            assertEquals(base, FDBExceptions.wrapException(e0));
            assertEquals(base, FDBExceptions.wrapException(e1));
            Throwable[] suppressedExceptions = base.getSuppressed();
            assertEquals(1, suppressedExceptions.length);
            assertThat(Arrays.asList(suppressedExceptions), not(anyOf(Matchers.<Throwable>hasItem(e0), hasItem(e1))));
            Throwable countException = suppressedExceptions[0];
            assertThat(countException, instanceOf(CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount.class));
            assertEquals(2, ((CompletionExceptionLogHelper.IgnoredSuppressedExceptionCount)countException).getCount());
        } finally {
            CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(Integer.MAX_VALUE); //cleanup
        }
    }

    @ResourceLock(value = COMPLETION_EXCEPTION_HELPER_LOCK, mode = ResourceAccessMode.READ_WRITE)
    @Test
    void negativeSuppressedExceptionLimit() {
        try {
            assertThrows(RecordCoreArgumentException.class, () ->
                    CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(-1));
        } finally {
            CompletionExceptionLogHelper.forceSetMaxSuppressedCountForTesting(Integer.MAX_VALUE); //cleanup, just in case
        }
    }

    private Exception createCompletionException(Exception cause) {
        return new CompletionException(PARENT_EXCEPTION_MESSAGE, cause);
    }

    @Nonnull
    private Exception createRuntimeException() {
        return new RuntimeException(EXCEPTION_CAUSE_MESSAGE);
    }

    @Nonnull
    private Exception createCheckedException() {
        return new Exception(EXCEPTION_CAUSE_MESSAGE);
    }

    @Nonnull
    private InterruptedException createInterruptedException() {
        return new InterruptedException(EXCEPTION_CAUSE_MESSAGE);
    }
}
