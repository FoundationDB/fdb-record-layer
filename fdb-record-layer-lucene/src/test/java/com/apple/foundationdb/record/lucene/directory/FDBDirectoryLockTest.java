/*
 * FDBDirectoryLockTest.java
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

import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class FDBDirectoryLockTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    private Subspace subspace;

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        subspace = fdb.run(path::toSubspace);
    }

    @ParameterizedTest
    @BooleanSource
    void testFileLock(boolean useAgile) throws IOException {
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext =
                    useAgile ?
                    AgilityContext.agile(context, 1000, 100_0000) :
                    AgilityContext.nonAgile(context);

            FDBDirectory directory = createDirectory(agilityContext);
            String lockName = "file.lock";
            String alreadyLockedMessage = "FileLock: Lock failed: already locked by another entity";
            final Lock lock1 = directory.obtainLock(lockName);
            lock1.ensureValid();
            LockObtainFailedException e = assertThrows(LockObtainFailedException.class, () -> directory.obtainLock(lockName));
            assertTrue(e.getMessage().contains(alreadyLockedMessage));
            lock1.ensureValid();
            lock1.close();

            assertThrows(AlreadyClosedException.class, lock1::ensureValid);
            final Lock lock2 = directory.obtainLock(lockName);
            lock2.ensureValid();
            e = assertThrows(LockObtainFailedException.class, () -> directory.obtainLock(lockName));
            assertTrue(e.getMessage().contains(alreadyLockedMessage));
            lock2.ensureValid();
            lock2.close();
        }
    }

    @Test
    void testFileLockCallback() throws IOException {
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext = AgilityContext.agile(context, 1000, 100_0000);

            FDBDirectory directory = createDirectory(agilityContext);
            final String lockName = "file.lock";
            final Lock lock1 = directory.obtainLock(lockName);
            final String string1 = lock1.toString();
            final byte[] firstKey = {1, 2, 3};
            final byte[] firstValue = {3, 2, 1};
            agilityContext.accept(aContext -> aContext.ensureActive().set(firstKey, firstValue));
            agilityContext.accept(aContext -> aContext.ensureActive().set(new byte[]{4, 5, 6}, new byte[]{6, 5, 4}));
            agilityContext.flush();
            final String string2 = lock1.toString();
            final byte[] bytes = agilityContext.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK, agilityContext.apply(aContext -> aContext.ensureActive().get(firstKey)));
            assertArrayEquals(firstValue, bytes);
            agilityContext.accept(aContext -> aContext.ensureActive().set(new byte[]{7, 8, 9}, new byte[]{9, 8, 7}));
            agilityContext.accept(aContext -> aContext.ensureActive().set(new byte[]{10, 11, 12}, new byte[]{12, 11, 10}));
            final String string22 = lock1.toString();

            assertEquals(string2, string22);

            agilityContext.flush();
            final String string3 = lock1.toString();

            assertNotEquals(string1, string2);
            assertNotEquals(string2, string3);
            assertNotEquals(string3, string1);

            lock1.ensureValid();
            lock1.close();
        }
    }

    @Test
    void testFileLockCallbackFrequently() throws IOException {
        // This test simulates the situation where the AgilityContext flushes as part of the call to clear
        final String lockName = "file.lock";
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext = AgilityContext.agile(context, 0, 0);
            try (FDBDirectory directory = createDirectory(agilityContext)) {
                final Lock lock1 = directory.obtainLock(lockName);
                lock1.close();
            }
            agilityContext.flushAndClose();
        }

        assertCanObtainLock(lockName);
    }

    @ParameterizedTest
    @BooleanSource
    void testFileLockCallbackFrequentlyLost(boolean clearLock) throws IOException {
        // This test simulates the situation where the AgilityContext flushes as part of the call to clear
        final String lockName = "file.lock";
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext = AgilityContext.agile(context, -5, 0);
            FDBDirectory directory = createDirectory(agilityContext);
            try {
                final Lock lock1 = directory.obtainLock(lockName);
                if (clearLock) {
                    forceClearLock(lockName);
                } else {
                    forceStealLock(lockName);
                }
                assertThrows(AlreadyClosedException.class, lock1::close);
            } finally {
                assertThrows(AlreadyClosedException.class, directory::close);
            }
            agilityContext.abortAndClose();
        }
        assertCanObtainLock(lockName);
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testFileLockClose(boolean useAgile, boolean abortAgilityContext) throws IOException {
        // Run multiple times to verify that a new lock can be obtained
        for (int i = 0; i < 3; i++) {
            try (FDBRecordContext context = fdb.openContext()) {
                AgilityContext agilityContext =
                        useAgile ?
                        AgilityContext.agile(context, 1000, 100_0000) :
                        AgilityContext.nonAgile(context);

                FDBDirectory directory = createDirectory(agilityContext);
                String lockName = "file.lock";
                final Lock lock1 = directory.obtainLock(lockName);
                lock1.ensureValid();
                if (abortAgilityContext) {
                    agilityContext.abortAndClose();
                    assertTrue(agilityContext.isClosed());
                } else {
                    assertFalse(agilityContext.isClosed());
                }
                lock1.close();
                directory.close();
                agilityContext.flushAndClose();
                context.commit();
            }
        }
    }

    private void assertCanObtainLock(final String lockName) throws IOException {
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext = AgilityContext.nonAgile(context);
            try (FDBDirectory directory = createDirectory(agilityContext)) {
                directory.obtainLock(lockName).close(); // should be able to obtain the lock
            }
            agilityContext.abortAndClose();
        }
    }

    private void forceClearLock(final String lockName) {
        try (FDBRecordContext context = fdb.openContext()) {
            final AgilityContext agilityContext = AgilityContext.nonAgile(context);
            try (FDBDirectory directory2 = createDirectory(agilityContext)) {
                agilityContext.accept(context2 -> {
                    context2.ensureActive().clear(directory2.fileLockKey(lockName));
                });
            }
            context.commit();
        }
    }

    private void forceStealLock(final String lockName) throws IOException {
        try (FDBRecordContext context = fdb.openContext()) {
            final AgilityContext agilityContext = AgilityContext.nonAgile(context);
            try (FDBDirectory directory = createDirectory(agilityContext)) {
                agilityContext.accept(context2 -> {
                    context2.ensureActive().clear(directory.fileLockKey(lockName));
                });
                directory.obtainLock(lockName);
            }
            context.commit();
        }
    }

    private @Nonnull FDBDirectory createDirectory(final AgilityContext agilityContext) {
        return new FDBDirectory(subspace, null, null, null, true, agilityContext);
    }
}
