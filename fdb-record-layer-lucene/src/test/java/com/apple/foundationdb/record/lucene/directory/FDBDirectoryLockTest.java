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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.test.Tags;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class FDBDirectoryLockTest extends FDBDirectoryBaseTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFileLock(boolean useAgile) throws IOException {
        try (FDBRecordContext context = fdb.openContext()) {
            AgilityContext agilityContext =
                    useAgile ?
                    AgilityContext.agile(context, 1000, 100_0000) :
                    AgilityContext.nonAgile(context);
            directory = new FDBDirectory(subspace, null, null, null, true, agilityContext);
            String lockName = "file.lock";
            String alreadyLockedMessage = "FileLock: Lock failed: already locked by another entity";
            final Lock lock1 = directory.obtainLock(lockName);
            lock1.ensureValid();
            RecordCoreException e = assertThrows(RecordCoreException.class, () -> directory.obtainLock(lockName));
            assertTrue(e.getMessage().contains(alreadyLockedMessage));
            lock1.ensureValid();
            lock1.close();
            assertThrows(AlreadyClosedException.class, lock1::ensureValid);
            final Lock lock2 = directory.obtainLock(lockName);
            lock2.ensureValid();
            e = assertThrows(RecordCoreException.class, () -> directory.obtainLock(lockName));
            assertTrue(e.getMessage().contains(alreadyLockedMessage));
            lock2.ensureValid();
            lock2.close();
        }
    }
}
