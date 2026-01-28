/*
 * NoOpLockFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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


import com.apple.foundationdb.annotation.API;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;

/**
 * A {@link LockFactory} that does not actually acquire locks. This is used for read-only
 * operations where the writer needs to be created but should not interfere with existing
 * locks (e.g., when replaying queued operations while a merge is in progress).
 *
 * <p>This factory returns a no-op lock that always succeeds without actually acquiring
 * any resources. Since no real locking occurs, this should only be used when we can
 * guarantee that no writes will be flushed or committed.
 */
@API(API.Status.INTERNAL)
public class NoOpLockFactory extends LockFactory {

    /**
     * Singleton instance of the NoOpLockFactory.
     */
    public static final NoOpLockFactory INSTANCE = new NoOpLockFactory();

    private NoOpLockFactory() {
        // Private constructor for singleton
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        return NoOpLock.INSTANCE;
    }

    /**
     * A no-op lock that always succeeds and does nothing on close.
     */
    private static class NoOpLock extends Lock {
        static final NoOpLock INSTANCE = new NoOpLock();

        private NoOpLock() {
            // Private constructor for singleton
        }

        @Override
        public void close() throws IOException {
            // No-op: nothing to release
        }

        @Override
        public void ensureValid() throws IOException {
            // No-op: always valid since we don't actually acquire locks
        }
    }
}
