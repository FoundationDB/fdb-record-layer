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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Produce a lock over {@link FDBDirectory}.
 */
public final class FDBDirectoryLockFactory extends LockFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDirectoryLockFactory.class);
    final FDBDirectory directory;

    public FDBDirectoryLockFactory(FDBDirectory directory) {
        this.directory = directory;
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        // dir is ignored
        return new FdbDirectoryLock(directory, lockName);
    }

    private static class FdbDirectoryLock extends Lock {

        final FDBDirectory directory;
        final byte[] key;
        final byte[] value;
        final String lockName;
        final long timeStampMillis;
        boolean closed;

        public FdbDirectoryLock(final FDBDirectory directory, final String lockName) {
            this.directory = directory;
            this.lockName = lockName;
            final Subspace subspace = directory.getSubspace();
            this.key = subspace.pack("fileLock:" + lockName);
            this.timeStampMillis = System.currentTimeMillis();
            this.value = subspace.pack(this.timeStampMillis);
            directory.fileLockSet(key, value);
            tellMe("Set lock");
        }

        @Override
        public void close() {
            directory.fileLockClear(key, value);
            closed = true;
            tellMe("Release lock");
        }

        @Override
        public void ensureValid() throws IOException {
            if (closed) {
                tellMe("validate: already close");
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }
            final Subspace subspace = directory.getSubspace();
            byte[] currentValue = directory.fileLockGet(key);
            if (currentValue == null) {
                tellMe("validate: null lock");
                throw new AlreadyClosedException("Lock file was deleted (lock=" + this + ")");
            }
            final long currentTimeStamp = subspace.unpack(currentValue).getLong(0);
            if (currentTimeStamp != timeStampMillis) {
                tellMe("validate: invalid lock", LogMessageKeys.TIME_STARTED_MILLIS, currentValue);
                throw new AlreadyClosedException("Lock file changed by an external force at " + currentTimeStamp + ", (lock=" + this + ")");
            }
        }

        @Override
        public String toString() {
            return "FDBDirectory: name=" + lockName + " timeMillis=" + timeStampMillis;
        }

        @Nonnull
        private void tellMe(@Nonnull String staticMsg, @Nullable final Object... keysAndValues) {
            if (false && LOGGER.isDebugEnabled()) {
                LOGGER.debug(KeyValueLogMessage.build("DirectoryLock: " + staticMsg, keysAndValues)
                        .addKeyAndValue(LogMessageKeys.INDEX_MERGE_LOCK, this)
                        .toString());
            }
        }
    }
}
