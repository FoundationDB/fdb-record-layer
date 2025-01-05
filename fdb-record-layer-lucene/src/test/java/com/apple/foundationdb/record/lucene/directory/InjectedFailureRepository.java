/*
 * InjectedFailureRepository.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A repository of injected failures.
 * This class is a collection of {@link FailureDescription} instances that can be used by the testing (Mocking) code to
 * inject failures at areas of the code as requested by the tests.
 */
public class InjectedFailureRepository {
    /**
     * The method descriptions that can fail execution.
     */
    public enum Methods {
        LUCENE_GET_INCREMENT,
        LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
        LUCENE_READ_BLOCK,
        LUCENE_LIST_ALL,
        LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC,
        LUCENE_DELETE_FILE_INTERNAL,
        LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
        LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX_FORCE_NULL,
        LUCENE_GET_ALL_FIELDS_INFO_STREAM
    }

    // The injected failure state
    private EnumMap<Methods, FailureDescription> failureDescriptions = new EnumMap<>(Methods.class);
    private EnumMap<Methods, AtomicLong> invocationCounts = new EnumMap<>(Methods.class);

    public void addFailure(@Nonnull Methods method, @Nonnull Exception exception, long count) {
        failureDescriptions.put(method, new FailureDescription(method, exception, count));
    }

    public void removeFailure(@Nonnull Methods method) {
        failureDescriptions.remove(method);
        invocationCounts.remove(method);
    }

    public void clear() {
        failureDescriptions.clear();
        invocationCounts.clear();
    }

    public void checkFailureForIoException(@Nonnull final Methods method) throws IOException {
        try {
            checkFailure(method);
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Expected IOException, got " + ex.getClass().getSimpleName(), ex);
        }
    }

    public void checkFailureForCoreException(@Nonnull final Methods method) throws RecordCoreException {
        try {
            checkFailure(method);
        } catch (RecordCoreException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Expected RecordCoreException, got " + ex.getClass().getSimpleName(), ex);
        }
    }

    private void checkFailure(@Nonnull final Methods method) throws Exception {
        // Local definitions have precedence over global ones
        FailureDescription failureDescription = failureDescriptions.get(method);
        if (failureDescription != null) {
            AtomicLong count = invocationCounts.computeIfAbsent(method, m -> new AtomicLong(0));
            long invocations = count.incrementAndGet();
            if (invocations > failureDescription.getCount()) {
                throw failureDescription.getException();
            }
        }
    }

    public boolean shouldFailWithoutException(@Nonnull Methods method) {
        // Return true "count" times, then return false
        // (Note that it's a reverse logic of "checkFailure" - which will succeed "count" times, then repeatedly throw an exception)
        FailureDescription failureDescription = failureDescriptions.get(method);
        if (failureDescription != null) {
            AtomicLong count = invocationCounts.computeIfAbsent(method, m -> new AtomicLong(0));
            long invocations = count.incrementAndGet();
            return invocations < failureDescription.getCount();
        }
        return false;
    }

    /**
     * A Failure description is the definition of the failure to be injected.
     */
    public static class FailureDescription {
        @Nonnull
        private final Methods method;
        @Nonnull
        private final Exception exception;
        private final long count;

        /**
         * Create a failure  desciption.
         * @param method the method that will fail: {@link Methods} should contain the entire list
         * @param exception the exception to throw
         * @param count the number of "clean" executions to allow before starting to throw the exception
         */
        public FailureDescription(@Nonnull final Methods method, @Nonnull final Exception exception, final long count) {
            this.method = method;
            this.exception = exception;
            this.count = count;
        }

        @Nonnull
        public Methods getMethod() {
            return method;
        }

        @Nonnull
        public Exception getException() {
            return exception;
        }

        public long getCount() {
            return count;
        }
    }
}
