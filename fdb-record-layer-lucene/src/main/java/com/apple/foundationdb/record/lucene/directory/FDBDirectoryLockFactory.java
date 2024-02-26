/*
 * FDBDirectoryLockFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Produce a lock over {@link FDBDirectory}.
 */
public final class FDBDirectoryLockFactory extends LockFactory {
    final FDBDirectory directory;
    final long timeWindowMilliseconds;

    public FDBDirectoryLockFactory(FDBDirectory directory, long timeWindowMilliseconds) {
        this.directory = directory;
        this.timeWindowMilliseconds = timeWindowMilliseconds;
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) {
        // dir is ignored
        return new FDBDirectoryLock(directory.getAgilityContext(), lockName, directory.fileLockKey(lockName), timeWindowMilliseconds);
    }

    private static class FDBDirectoryLock extends Lock {

        final AgilityContext agilityContext;
        final String lockName;
        final long timeStampMillis;
        final long timeWindowMilliseconds;
        final byte[] fileLockKey;
        boolean closed;
        private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryLock.class);

        public FDBDirectoryLock(final AgilityContext agilityContext, final String lockName, byte[] fileLockKey, long timeWindowMilliseconds) {
            this.agilityContext = agilityContext;
            this.lockName = lockName; // for log messages
            this.fileLockKey = fileLockKey;
            this.timeWindowMilliseconds = timeWindowMilliseconds;
            this.timeStampMillis = System.currentTimeMillis();
            fileLockSet(timeStampMillis);
        }

        @Override
        public void close() {
            fileLockClear(timeStampMillis);
            closed = true;
        }

        @Override
        public void ensureValid() {
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released. This=" + this);
            }
            final long now = System.currentTimeMillis();
            if (now > timeStampMillis + timeWindowMilliseconds) {
                throw new AlreadyClosedException("Lock is too old. This=" + this + " now=" + now);
            }
            long existingValue = fileLockGet();
            if (existingValue == 0) {
                throw new AlreadyClosedException("Lock file was deleted. This=" + this);
            }
            if (existingValue != timeStampMillis) {
                throw new AlreadyClosedException("Lock file changed by an external force at " + existingValue + ". This=" + this);
            }
        }


        private static byte[] fileLockValue(long timeStampMillis) {
            return Tuple.from(timeStampMillis).pack();
        }

        private static long fileLockValueToTimestamp(byte[] value) {
            return value == null ? 0 :
                   Tuple.fromBytes(value).getLong(0);
        }

        public void fileLockSet(long timeStampMillis) {
            byte[] value = fileLockValue(timeStampMillis);
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_SET,
                    agilityContext.apply(aContext ->
                            aContext.ensureActive().get(fileLockKey)
                                    .thenAccept(val -> {
                                        if (val != null) {
                                            long existingTimeStamp =  fileLockValueToTimestamp(val);
                                            if (existingTimeStamp > (timeStampMillis - timeWindowMilliseconds) &&
                                                    existingTimeStamp < (timeStampMillis + timeWindowMilliseconds)) {
                                                // Here: this lock is valid
                                                throw new RecordCoreException("FileLock: Set: found old lock")
                                                        .addLogInfo(LuceneLogMessageKeys.LOCK_EXISTING_TIMESTAMP, existingTimeStamp,
                                                                LuceneLogMessageKeys.LOCK_DIRECTORY, this);
                                            }
                                            // Here: this lock is either too old, or in the future. Steal it
                                            if (LOGGER.isWarnEnabled()) {
                                                LOGGER.warn(KeyValueLogMessage.of("FileLock: Set: found old lock, discard it",
                                                        LuceneLogMessageKeys.LOCK_EXISTING_TIMESTAMP, existingTimeStamp,
                                                        LuceneLogMessageKeys.LOCK_DIRECTORY, this));
                                            }
                                        }
                                        aContext.ensureActive().set(fileLockKey, value);
                                    })
                    ));
        }

        public long fileLockGet() {
            // return time stamp if exists, 0 if not
            byte[] val = agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_GET, agilityContext.get(fileLockKey));
            return fileLockValueToTimestamp(val);
        }

        public void fileLockClear(long timeStampMillis) {
            byte[] value = fileLockValue(timeStampMillis);
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_CLEAR,
                    agilityContext.apply(aContext ->
                            aContext.ensureActive().get(fileLockKey)
                                    .thenAccept(val -> {
                                        if (!Arrays.equals(value, val)) {
                                            throw new RecordCoreException("FileLock: Clear: found unexpected lock")
                                                    .addLogInfo(LogMessageKeys.ACTUAL, val,
                                                            LuceneLogMessageKeys.LOCK_EXISTING_TIMESTAMP, fileLockValueToTimestamp(val),
                                                            LuceneLogMessageKeys.LOCK_DIRECTORY, this);
                                        }
                                        aContext.ensureActive().clear(fileLockKey);
                                    })
                    ));
        }



        @Override
        public String toString() {
            return "FDBDirectoryLock: name=" + lockName + " timeMillis=" + timeStampMillis;
        }
    }
}
