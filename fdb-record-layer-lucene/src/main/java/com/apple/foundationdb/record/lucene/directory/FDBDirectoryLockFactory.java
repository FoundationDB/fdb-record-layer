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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

/**
 * Produce a lock over {@link FDBDirectory}.
 */
public final class FDBDirectoryLockFactory extends LockFactory {
    final FDBDirectory directory;

    public FDBDirectoryLockFactory(FDBDirectory directory) {
        this.directory = directory;
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        // dir is ignored
        return new FDBDirectoryLock(directory, lockName);
    }

    private static class FDBDirectoryLock extends Lock {

        final FDBDirectory directory;
        final String lockName;
        final long timeStampMillis;
        boolean closed;

        public FDBDirectoryLock(final FDBDirectory directory, final String lockName) {
            this.directory = directory;
            this.lockName = lockName;
            this.timeStampMillis = System.currentTimeMillis();
            directory.fileLockSet(lockName, timeStampMillis);
        }

        @Override
        public void close() {
            directory.fileLockClear(lockName, timeStampMillis);
            closed = true;
        }

        @Override
        public void ensureValid() throws IOException {
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }
            long existingValue = directory.fileLockGet(lockName);
            if (existingValue == 0) {
                throw new AlreadyClosedException("Lock file was deleted (lock=" + this + ")");
            }
            if (existingValue != timeStampMillis) {
                throw new AlreadyClosedException("Lock file changed by an external force at " + existingValue + ", (lock=" + this + ")");
            }
        }

        @Override
        public String toString() {
            return "FDBDirectory: name=" + lockName + " timeMillis=" + timeStampMillis;
        }
    }
}
