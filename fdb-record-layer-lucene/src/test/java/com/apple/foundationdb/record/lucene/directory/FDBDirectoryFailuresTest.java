/*
 * FDBDirectoryFailuresTest.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.lucene.LuceneConcurrency;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_DELETE_FILE_INTERNAL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_INCREMENT;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_LIST_ALL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_READ_BLOCK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a modified version of the {@link FDBDirectoryTest} where a mocked directory is set up and various FDBDirectory
 * calls are mocked to fail. Assertions on the correct exceptions are in place.
 */
@Tag(Tags.RequiresFDB)
public class FDBDirectoryFailuresTest extends FDBDirectoryBaseTest {
    private InjectedFailureRepository injectedFailures;

    @Test
    public void testGetIncrement() throws IOException {
        addFailure(LUCENE_GET_INCREMENT, new IOException("Blah"), 2);

        assertEquals(1, directory.getIncrement());
        assertEquals(2, directory.getIncrement());
        // This call fails with the mock exveption
        assertThrows(IOException.class, () -> directory.getIncrement());
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS, 2);
        directory.getCallerContext().commit();

        try (FDBRecordContext context = fdb.openContext()) {
            directory = new FDBDirectory(subspace, context, null);
            assertEquals(3, directory.getIncrement());
        }
    }

    @Test
    public void testWriteLuceneFileReference() {
        addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                0);

        // write already created file reference
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(2, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(3, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference2);
        // This call should fail witht the mock exception
        Exception ex = assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class, () -> directory.getFDBLuceneFileReference("test1"));
        assertTrue(ex.getCause() instanceof TimeoutException);
    }

    @Test
    public void testWriteSeekData() throws Exception {
        addFailure(LUCENE_READ_BLOCK,
                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                0);

        directory.writeFDBLuceneFileReference("testReference1", new FDBLuceneFileReference(1, 1, 1, 1));
        final EmptyIndexInput emptyInput = new EmptyIndexInput("Empty Input");
        // This call should fail with the mock exception
        // Future.get throws ExecutionException
        Exception ex = assertThrows(ExecutionException.class,
                () -> directory.readBlock(emptyInput, "testReference1", directory.getFDBLuceneFileReferenceAsync("testReference1"), 1)
                        .get());
        assertTrue(ex.getCause() instanceof LuceneConcurrency.AsyncToSyncTimeoutException);

        directory.writeFDBLuceneFileReference("testReference2", new FDBLuceneFileReference(2, 1, 1, 200));
        byte[] data = "test string for write".getBytes();
        directory.writeData(2, 1, data);
        // This call should fail with the mock exception
        // AsyncToSync restored the original timeout exception
        ex = assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK,
                        directory.readBlock(emptyInput, "testReference2", directory.getFDBLuceneFileReferenceAsync("testReference2"), 1),
                        context));
        assertTrue(ex.getCause() instanceof TimeoutException);

        directory.getCallerContext().commit();
        assertCorrectMetricSize(LuceneEvents.SizeEvents.LUCENE_WRITE, 1, directory.getSerializer().encode(data, true, false).length);
    }

    @Test
    public void testLitAllGetFileReference() throws IOException {
        addFailure(LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC,
                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                0);

        // This call should fail with the mock exception
        Exception ex = assertThrows(IOException.class, () -> directory.listAll());
        assertTrue(ex.getCause() instanceof LuceneConcurrency.AsyncToSyncTimeoutException);
    }

    @Test
    public void testListAll() throws IOException {
        addFailure(LUCENE_LIST_ALL, new IOException("Blah"), 0);

        // This call should fail with the mock exception
        assertThrows(IOException.class, () -> directory.listAll());
        directory.writeFDBLuceneFileReference("test1", new FDBLuceneFileReference(1, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test2", new FDBLuceneFileReference(2, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test3", new FDBLuceneFileReference(3, 1, 1, 1));

        // clear failure, listAll() should pass now
        removeFailure(LUCENE_LIST_ALL);
        assertArrayEquals(new String[] {"test1", "test2", "test3"}, directory.listAll());
        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_LIST_ALL, 1);
        // Assert that the cache is loaded only once even though directory::listAll is called twice
        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_LOAD_FILE_CACHE, 1);
        directory.getCallerContext().ensureActive().cancel();

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            directory = new FDBDirectory(subspace, context, null);
            assertArrayEquals(new String[0], directory.listAll());
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_LIST_ALL));
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_LOAD_FILE_CACHE));
        }
    }

    @Test
    public void testDeleteData() throws Exception {
        addFailure(LUCENE_DELETE_FILE_INTERNAL,
                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException()),
                1);

        assertThrows(NoSuchFileException.class, () -> directory.deleteFile("NonExist"));
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        // This should fail with mock exception
        Exception ex = assertThrows(IOException.class, () -> directory.deleteFile("test1"));
        assertTrue(ex.getCause() instanceof LuceneConcurrency.AsyncToSyncTimeoutException);
        assertEquals(directory.listAll().length, 1);

        // WAIT only gets called if there's a future to wait on, and so this value can be less than 2 if
        // the futures complete quickly
        assertMetricCountAtMost(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE, 1);
    }

    @Test
    public void testFileLength() throws Exception {
        addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                new FDBExceptions.FDBStoreTransactionIsTooOldException("Blah", new FDBException("Blah", 7)),
                0);

        long expectedSize = 20;
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, expectedSize, expectedSize, 1024);
        directory.writeFDBLuceneFileReference("test1", reference1);
        // This call should fail with mocked exception
        // This is the IOException flavor of the exception
        assertThrows(LuceneExceptions.LuceneTransactionTooOldException.class, () -> directory.fileLength("test1"));

        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_GET_FILE_LENGTH, 1);
        directory.getCallerContext().commit();

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = fdb.openContext(null, timer)) {
            directory = new FDBDirectory(subspace, context, null);
            long fileSize2 = directory.fileLength("test1");
            assertEquals(expectedSize, fileSize2);
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_GET_FILE_LENGTH));
            assertEquals(1, timer.getCount(LuceneEvents.Waits.WAIT_LUCENE_FILE_LENGTH));
        }
    }

    @Test
    public void testRename() {
        addFailure(LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC,
                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                0);

        final IOException ioException = assertThrows(IOException.class, () -> directory.rename("NoExist", "newName"));
        assertTrue(ioException.getCause() instanceof LuceneConcurrency.AsyncToSyncTimeoutException);

        // In the case where the Future was already completed, WAIT_LUCENE_RENAME will not be recorded, but LUCENE_RENAME_FILE will
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_RENAME_FILE, 1);
    }

    private void assertCorrectMetricCount(StoreTimer.Event metric, int expectedValue) {
        assertEquals(expectedValue, timer.getCount(metric),
                () -> "Incorrect call count for metric " + metric);
    }

    private void assertCorrectMetricSize(StoreTimer.SizeEvent metric, int expectedNumber, long expectedCumulativeBytes) {
        assertEquals(expectedNumber, timer.getCount(metric),
                () -> "Incorrect call count for metric " + metric);
        assertEquals(expectedCumulativeBytes, timer.getSize(metric),
                () -> "Incorrect size for metric " + metric);
    }

    private void assertMetricCountAtMost(StoreTimer.Event metric, int maximumValue) {
        assertThat("Metric " + metric + " should be called at most " + maximumValue + " times",
                timer.getCount(metric), lessThanOrEqualTo(maximumValue));
    }

    /*
     * Override default behavior to create a mocked directory.
     */
    @Nonnull
    @Override
    protected FDBDirectory createDirectory(final Subspace subspace, final FDBRecordContext context, final Map<String, String> indexOptions) {
        injectedFailures = new InjectedFailureRepository();
        final MockedFDBDirectory directory = new MockedFDBDirectory(subspace, context, indexOptions);
        directory.setInjectedFailures(injectedFailures);
        return directory;
    }

    private void addFailure(final InjectedFailureRepository.Methods method, final Exception exception, final int count) {
        injectedFailures.addFailure(method, exception, count);
    }

    private void removeFailure(final InjectedFailureRepository.Methods method) {
        injectedFailures.removeFailure(method);
    }
}
