/*
 * FDBExceptions.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreInterruptedException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Namespace for exceptions that wrap the underlying exceptions from the FDB API.
 */
@API(API.Status.STABLE)
public class FDBExceptions {

    private FDBExceptions() {
    }

    /**
     * Exceptions that are reported by (or due to limitations of, etc.) the FDB API.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreException extends RecordCoreStorageException {
        public FDBStoreException(String message) {
            super(message);
        }

        public FDBStoreException(String message, @Nullable Object... keyValues) {
            super(message, keyValues);
        }

        public FDBStoreException(FDBException cause) {
            super(cause.getMessage(), cause);
        }
    }

    /**
     * Exception thrown when transaction size is exceeded.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreTransactionSizeException extends FDBStoreException {
        public FDBStoreTransactionSizeException(FDBException cause) {
            super(cause);
        }
    }

    /**
     * Exception thrown when key size is exceeded.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreKeySizeException extends FDBStoreException {
        public FDBStoreKeySizeException(String message, @Nullable Object... keyValues) {
            super(message, keyValues);
        }

        public FDBStoreKeySizeException(FDBException cause) {
            super(cause);
        }
    }

    /**
     * Exception thrown when value size is exceeded.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreValueSizeException extends FDBStoreException {
        public FDBStoreValueSizeException(String message, @Nullable Object... keyValues) {
            super(message, keyValues);
        }

        public FDBStoreValueSizeException(FDBException cause) {
            super(cause);
        }
    }

    /**
     * Transaction is too old to perform reads or be committed.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreTransactionIsTooOldException extends FDBStoreRetriableException {
        public FDBStoreTransactionIsTooOldException(FDBException cause) {
            super(cause);
        }
    }

    /**
     * Transaction failed due to a conflict with another transaction.
     */
    @SuppressWarnings("serial")
    public static class FDBStoreTransactionConflictException extends FDBStoreRetriableException {
        public FDBStoreTransactionConflictException(FDBException cause) {
            super(cause);
        }
    }

    /**
     * An exception that should be retried by the caller because is stems from a transient condition in FDB.
     * @see FDBException#isRetryable
     */
    @SuppressWarnings("serial")
    public static class FDBStoreRetriableException extends RecordCoreRetriableTransactionException {
        public FDBStoreRetriableException(FDBException cause) {
            super(cause.getMessage(), cause);
        }
    }

    public static RuntimeException wrapException(@Nonnull Throwable ex) {
        if (ex instanceof CompletionException) {
            ex = CompletionExceptionLogHelper.asCause((CompletionException)ex);
        } else if (ex instanceof ExecutionException) {
            ex = CompletionExceptionLogHelper.asCause((ExecutionException)ex);
        }
        if (ex instanceof FDBException) {
            FDBException fdbex = (FDBException)ex;
            switch (fdbex.getCode()) {
                case 1007:    // transaction_too_old
                    return new FDBStoreTransactionIsTooOldException(fdbex);
                case 1020:    // not_committed
                    return new FDBStoreTransactionConflictException(fdbex);
                case 2101:    // transaction_too_large
                    return new FDBStoreTransactionSizeException(fdbex);
                case 2102:    // key_too_large
                    return new FDBStoreKeySizeException(fdbex);
                case 2103:    // value_too_large
                    return new FDBStoreValueSizeException(fdbex);
                default:
                    if (fdbex.isRetryable()) {
                        return new FDBStoreRetriableException(fdbex);
                    } else {
                        return new FDBStoreException(fdbex);
                    }
            }
        }
        if (ex instanceof RuntimeException) {
            return (RuntimeException)ex;
        }
        if (ex instanceof InterruptedException) {
            return new RecordCoreInterruptedException(ex.getMessage(), ex);
        }
        return new RecordCoreException(ex.getMessage(), ex);
    }

    /**
     * Returns whether an exception is retriable or is caused by a retriable error.
     * This will walk up the exception hierarchy and return <code>true</code> if
     * it finds an exception that it knows to be retriable. This could occur, for
     * example, if the given exception is caused by a retriable {@link FDBException}
     * such as a network failure or a recovery.
     *
     * @param ex the exception to check for a retriable cause
     * @return <code>true</code> if this exception was caused by something retriable
     *  and <code>false</code> otherwise
     */
    public static boolean isRetriable(@Nullable Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof RecordCoreRetriableTransactionException) {
                return true;
            } else if (current instanceof FDBException) {
                return ((FDBException)current).isRetryable();
            }
            current = current.getCause();
        }
        return false;
    }
}
