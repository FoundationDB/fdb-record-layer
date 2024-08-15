/*
 * LuceneExceptions.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryLockFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;

/**
 * Utility class for converting Lucene Exceptions to/from Record layer ones.
 */
public class LuceneExceptions {
    /**
     * Convert the exception thrown by Lucene by a {@link RecordCoreException} that can be later interpreted by the higher levels.
     * @param message the exception's message to use; the cause's message will be appended to this one
     * @param ex the exception thrown by Lucene
     * @param additionalLogInfo (optional) additional log infos to add to the created exception
     * @return the {@link RecordCoreException} that should be thrown
     */
    public static RecordCoreException toRecordCoreException(String message, IOException ex, Object... additionalLogInfo) {
        if (ex instanceof LockObtainFailedException) {
            // Use the retryable exception for this case
            return new FDBExceptions.FDBStoreLockTakenException(message + ": " + ex.getMessage(), ex)
                    .addLogInfo(additionalLogInfo);
        } else if (ex instanceof LuceneTransactionTooOldException) {
            // Use the standard retryable exception
            Throwable cause = ex.getCause();
            // Normally that would wrap the actual transaction-too-long from FDB
            if (cause instanceof FDBExceptions.FDBStoreTransactionIsTooOldException) {
                return (FDBExceptions.FDBStoreTransactionIsTooOldException)cause;
            } else {
                // This should not happen - LuceneTransactionTooOldException should have FDBStoreTransactionIsTooOldException as cause
                RecordCoreException result = new FDBExceptions.FDBStoreTransactionIsTooOldException(message + ": " + ex.getMessage(), null)
                        .addLogInfo(additionalLogInfo);
                result.addSuppressed(ex);
                return result;
            }
        }

        return new RecordCoreException(message + ": " + ex.getMessage(), ex)
                .addLogInfo(additionalLogInfo);
    }

    /**
     * Convert an exception thrown by the lower levels to one that can be thrown by Lucene ({@link IOException}).
     * @param ex the exception thrown by FDB
     * @return the {@link IOException} that can be thrown through Lucene APIs
     */
    public static IOException toIoException(Throwable ex, Throwable suppressed) {
        IOException result;
        if (ex instanceof FDBExceptions.FDBStoreTransactionIsTooOldException) {
            result = new LuceneExceptions.LuceneTransactionTooOldException((FDBExceptions.FDBStoreTransactionIsTooOldException)ex);
        } else if (ex instanceof FDBDirectoryLockFactory.FDBDirectoryLockException) {
            result = new LockObtainFailedException(ex.getMessage(), ex);
        } else if (ex instanceof IOException) {
            result = (IOException)ex;
        } else {
            result = new IOException(ex);
        }

        if (suppressed != null) {
            result.addSuppressed(suppressed);
        }
        return result;
    }

    private LuceneExceptions() {
    }

    /**
     * A Wrapper around the transaction-too-old exception that gets thrown through Lucene as an IOException.
     * Once received, it can be translated back into a {@link FDBExceptions.FDBStoreTransactionIsTooOldException}
     */
    public static class LuceneTransactionTooOldException extends IOException {
        private static final long serialVersionUID = -1L;

        public LuceneTransactionTooOldException(final String message) {
            super(message);
        }

        public LuceneTransactionTooOldException(final FDBExceptions.FDBStoreTransactionIsTooOldException cause) {
            super(cause);
        }

        public LuceneTransactionTooOldException(final String message, final FDBExceptions.FDBStoreTransactionIsTooOldException cause) {
            super(message, cause);
        }
    }
}
