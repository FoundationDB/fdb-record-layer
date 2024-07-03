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
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.IOException;

/**
 * Utility class for converting Lucene Exceptions to Record layer ones.
 */
public class LuceneExceptions {
    /**
     * Wrap the exception thrown by Lucene by a {@link RecordCoreException} that can be later interpreted by the higher levels.
     * @param message the exception's message to use; the cause's message will be appended to this one
     * @param ex the exception thrown by Lucene
     * @param additionalLogInfo (optional) additional log infos to add to the created exception
     * @return the {@link RecordCoreException} that should be thrown
     */
    public static RecordCoreException wrapException(String message, IOException ex, Object... additionalLogInfo) {
        if (ex instanceof LockObtainFailedException) {
            // Use the retryable exception for this case
            return new FDBExceptions.FDBStoreLockTakenException(message + ": " + ex.getMessage(), ex)
                    .addLogInfo(additionalLogInfo);
        }

        return new RecordCoreException(message + ": " + ex.getMessage(), ex)
                .addLogInfo(additionalLogInfo);
    }

    private LuceneExceptions() {
    }
}
