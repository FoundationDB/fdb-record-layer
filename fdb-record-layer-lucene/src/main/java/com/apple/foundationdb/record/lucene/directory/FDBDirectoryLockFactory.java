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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Produce a lock over {@link FDBDirectory}.
 */
public final class FDBDirectoryLockFactory extends LockFactory {
    final FDBDirectory directory;
    final int timeWindowMilliseconds;

    public FDBDirectoryLockFactory(FDBDirectory directory, int timeWindowMilliseconds) {
        this.directory = directory;
        this.timeWindowMilliseconds = timeWindowMilliseconds > TimeUnit.SECONDS.toMillis(10) ? timeWindowMilliseconds : (int) TimeUnit.MINUTES.toMillis(10);
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) {
        // dir is ignored
        return new FDBDirectoryLock(directory.getAgilityContext(), lockName, directory.fileLockKey(lockName), timeWindowMilliseconds);
    }

    private static class FDBDirectoryLock extends Lock {

        private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryLock.class);
        final AgilityContext agilityContext;
        final String lockName;
        final UUID selfStampUuid = UUID.randomUUID();
        long timeStampMillis;
        final int timeWindowMilliseconds;
        final byte[] fileLockKey;
        boolean closed;
        private final Object fileLockSetLock = new Object();

        public FDBDirectoryLock(final AgilityContext agilityContext, final String lockName, byte[] fileLockKey, int timeWindowMilliseconds) {
            this.agilityContext = agilityContext;
            this.lockName = lockName; // for log messages
            this.fileLockKey = fileLockKey;
            this.timeWindowMilliseconds = timeWindowMilliseconds;
            logSelf("Creating Lucene File Lock");
            fileLockSet(false);
        }

        @Override
        public void close() {
            fileLockClear();
            closed = true;
        }

        @Override
        public void ensureValid() {
            // .. and implement heartbeat
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released. This=" + this);
            }
            final long now = System.currentTimeMillis();
            if (now > timeStampMillis + timeWindowMilliseconds) {
                throw new AlreadyClosedException("Lock is too old. This=" + this + " now=" + now);
            }
            if (!agilityContext.isClosed()) {
                fileLockSet(true);
            }
        }


        private byte[] fileLockValue() {
            return Tuple.from(selfStampUuid, timeStampMillis).pack();
        }

        private static long fileLockValueToTimestamp(byte[] value) {
            return value == null ? 0 :
                   Tuple.fromBytes(value).getLong(1);
        }

        private static UUID fileLockValueToUuid(byte[] value) {
            return value == null ? null :
                   Tuple.fromBytes(value).getUUID(0);
        }

        public void fileLockSet(boolean isHeartbeat) {
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_SET,
                    agilityContext.apply(aContext -> fileLockSet(isHeartbeat, aContext)
                    ));
        }

        private CompletableFuture<Void> fileLockSet(boolean isHeartbeat, FDBRecordContext aContext) {
            final long nowMillis = System.currentTimeMillis();
            return aContext.ensureActive().get(fileLockKey)
                    .thenAccept(val -> {
                        synchronized (fileLockSetLock) {
                            if (isHeartbeat) {
                                fileLockCheckHeartBeat(val);
                            } else {
                                fileLockCheckNewLock(val, nowMillis);
                            }
                            this.timeStampMillis = nowMillis;
                            byte[] value = fileLockValue();
                            aContext.ensureActive().set(fileLockKey, value);
                        }
                    });
        }

        private void fileLockCheckHeartBeat(byte[] val) {
            long existingTimeStamp = fileLockValueToTimestamp(val);
            UUID existingUuid = fileLockValueToUuid(val);
            if (existingTimeStamp == 0 || existingUuid == null) {
                throw new AlreadyClosedException("Lock file was deleted. This=" + this);
            }
            if (existingUuid.compareTo(selfStampUuid) != 0) {
                throw new AlreadyClosedException("Lock file changed by " + existingUuid + " at " + existingTimeStamp + ". This=" + this);
            }
        }

        private void fileLockCheckNewLock(byte[] val, long nowMillis) {
            long existingTimeStamp = fileLockValueToTimestamp(val);
            UUID existingUuid = fileLockValueToUuid(val);
            if (existingUuid == null || existingTimeStamp <= 0) {
                // all clear
                return;
            }
            if (existingTimeStamp > (nowMillis - timeWindowMilliseconds) &&
                    existingTimeStamp < (nowMillis + timeWindowMilliseconds)) {
                // Here: this lock is valid
                throw new RecordCoreException("FileLock: Set: found old lock")
                        .addLogInfo(LuceneLogMessageKeys.LOCK_EXISTING_TIMESTAMP, existingTimeStamp,
                                LuceneLogMessageKeys.LOCK_EXISTING_UUID, existingUuid,
                                LuceneLogMessageKeys.LOCK_DIRECTORY, this);
            }
            // Here: this lock is either too old, or in the future. Steal it
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(KeyValueLogMessage.of("FileLock: Set: found old lock, discard it",
                        LuceneLogMessageKeys.LOCK_EXISTING_TIMESTAMP, existingTimeStamp,
                        LuceneLogMessageKeys.LOCK_DIRECTORY, this));
            }
        }


        private void fileLockClear() {
            agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_CLEAR,
                    agilityContext.apply(aContext ->
                            aContext.ensureActive().get(fileLockKey)
                                    .thenAccept(val -> {
                                        synchronized (fileLockSetLock) {
                                            fileLockCheckHeartBeat(val); // ensure valid
                                            aContext.ensureActive().clear(fileLockKey);
                                            logSelf("Cleared Lucene File Lock");
                                        }
                                    })
                    ));
        }

        @Override
        public String toString() {
            return "{FDBDirectoryLock: name=" + lockName + " uuid=" + selfStampUuid + " timeMillis=" + timeStampMillis + "}";
        }

        private void logSelf(String staticMessage) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.of(staticMessage,
                        LogMessageKeys.TIME_LIMIT_MILLIS, timeWindowMilliseconds,
                        LuceneLogMessageKeys.LOCK_TIMESTAMP, timeStampMillis,
                        LuceneLogMessageKeys.LOCK_UUID, selfStampUuid,
                        LogMessageKeys.KEY, ByteArrayUtil2.loggable(fileLockKey)));
            }

        }
    }
}
