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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreInterruptedException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.CompletionExceptionLogHelper;
import com.apple.foundationdb.util.LoggableKeysAndValues;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Namespace for exceptions that wrap the underlying exceptions from the FDB API.
 */
@API(API.Status.STABLE)
public class FDBExceptions {

    private static final Object[] EMPTY_KEYS_AND_VALUES = new Object[0];

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
     * Exception thrown when a transaction times out. This refers specifically to timeouts enforced by the
     * FoundationDB client, which can be configured by either setting the transaction timeout milliseconds
     * on the {@link FDBDatabaseFactory}, the {@link FDBDatabaseRunner}, or the {@link FDBRecordContextConfig}.
     *
     * <p>
     * Note that this exception is not retriable, and note also that it is not a child of {@link java.util.concurrent.TimeoutException}.
     * </p>
     *
     * @see FDBDatabaseFactory#setTransactionTimeoutMillis(long)
     * @see FDBDatabaseRunner#setTransactionTimeoutMillis(long)
     * @see FDBRecordContextConfig.Builder#setTransactionTimeoutMillis(long)
     * @see FDBRecordContext#getTimeoutMillis()
     */
    @SuppressWarnings("serial")
    public static class FDBStoreTransactionTimeoutException extends FDBStoreException {
        public FDBStoreTransactionTimeoutException(FDBException cause) {
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
        if (ex instanceof RecordCoreException) {
            return (RecordCoreException) ex;
        }

        //transfer any logging details into mapped exception
        Object[] logInfo;
        if (ex instanceof LoggableKeysAndValues) {
            logInfo = ((LoggableKeysAndValues)ex).exportLogInfo();
        } else {
            logInfo = EMPTY_KEYS_AND_VALUES;
        }
        if (ex instanceof CompletionException) {
            ex = CompletionExceptionLogHelper.asCause((CompletionException)ex);
        } else if (ex instanceof ExecutionException) {
            ex = CompletionExceptionLogHelper.asCause((ExecutionException)ex);
        }
        if (ex instanceof FDBException) {
            FDBException fdbex = (FDBException)ex;
            switch (FDBError.fromCode(fdbex.getCode())) {
                case TRANSACTION_TOO_OLD:
                    return new FDBStoreTransactionIsTooOldException(fdbex).addLogInfo(logInfo);
                case NOT_COMMITTED:
                    return new FDBStoreTransactionConflictException(fdbex).addLogInfo(logInfo);
                case TRANSACTION_TIMED_OUT:
                    return new FDBStoreTransactionTimeoutException(fdbex).addLogInfo(logInfo);
                case TRANSACTION_TOO_LARGE:
                    return new FDBStoreTransactionSizeException(fdbex).addLogInfo(logInfo);
                case KEY_TOO_LARGE:
                    return new FDBStoreKeySizeException(fdbex).addLogInfo(logInfo);
                case VALUE_TOO_LARGE:
                    return new FDBStoreValueSizeException(fdbex).addLogInfo(logInfo);
                default:
                    if (fdbex.isRetryable()) {
                        return new FDBStoreRetriableException(fdbex).addLogInfo(logInfo);
                    } else {
                        return new FDBStoreException(fdbex).addLogInfo(logInfo);
                    }
            }
        }
        if (ex instanceof RuntimeException) {
            return (RuntimeException)ex;
        }

        if (ex instanceof InterruptedException) {
            return new RecordCoreInterruptedException(ex.getMessage(), ex).addLogInfo(logInfo);
        }
        return new RecordCoreException(ex.getMessage(), ex).addLogInfo(logInfo);
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

    @Nullable
    public static FDBException getFDBCause(@Nullable Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof FDBException) {
                return (FDBException)current;
            }
            current = current.getCause();
        }
        return null;
    }
}
