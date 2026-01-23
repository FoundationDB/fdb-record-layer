/*
 * FDBDirectoryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.LuceneConcurrency;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for FDBDirectory validating it can function as a backing store
 * for Lucene.
 */
@Tag(Tags.RequiresFDB)
public class FDBDirectoryTest extends FDBDirectoryBaseTest {

    @Test
    public void testDirectoryCreate() {
        assertNotNull(directory, "directory should not be null");
        assertEquals(subspace, directory.getSubspace());
    }

    @Test
    public void testGetIncrement() throws IOException {
        assertEquals(1, directory.getIncrement());
        assertEquals(2, directory.getIncrement());
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS, 2);
        directory.getCallerContext().commit();

        try (FDBRecordContext context = fdb.openContext()) {
            directory = new FDBDirectory(subspace, context, null);
            assertEquals(3, directory.getIncrement());
        }
    }

    @Test
    public void testConcurrentGetIncrement() throws IOException {
        final int threads = 50;
        List<CompletableFuture<Long>> futures = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return directory.getIncrement();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, directory.getCallerContext().getExecutor()));
        }
        List<Long> values = AsyncUtil.getAll(futures).join();
        assertThat(values, containsInAnyOrder(LongStream.range(1, threads + 1).mapToObj(Matchers::equalTo).collect(Collectors.toList())));
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS, threads);
        assertMetricCountAtMost(LuceneEvents.Waits.WAIT_LUCENE_GET_INCREMENT, threads);
        directory.getCallerContext().commit();

        try (FDBRecordContext context = fdb.openContext()) {
            directory = new FDBDirectory(subspace, context, null);
            assertEquals(threads + 1, directory.getIncrement());
        }
    }

    @Test
    public void testWriteGetLuceneFileReference() {
        FDBLuceneFileReference luceneFileReference = directory.getFDBLuceneFileReference("NonExist");
        assertNull(luceneFileReference);
        String luceneReference1 = "luceneReference1";
        FDBLuceneFileReference fileReference = new FDBLuceneFileReference(1, 10, 10, 10);
        directory.writeFDBLuceneFileReference(luceneReference1, fileReference);
        FDBLuceneFileReference actual = directory.getFDBLuceneFileReference(luceneReference1);
        assertNotNull(actual, "file reference should exist");
        assertEquals(actual, fileReference);
    }

    @Test
    public void testWriteLuceneFileReference() {
        // write already created file reference
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(2, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(3, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference2);
        FDBLuceneFileReference luceneFileReference = directory.getFDBLuceneFileReference("test1");
        assertNotNull(luceneFileReference, "fileReference should exist");

        LuceneSerializer serializer = directory.getSerializer();
        assertCorrectMetricSize(LuceneEvents.SizeEvents.LUCENE_WRITE_FILE_REFERENCE, 2,
                serializer.encode(reference1.getBytes()).length + serializer.encode(reference2.getBytes()).length);
    }

    @Test
    public void testMissingSeek() {
        final CompletableFuture<byte[]> seekFuture = directory.readBlock(
                new EmptyIndexInput("Test Empty"),
                "testDescription",
                directory.getFDBLuceneFileReferenceAsync("testReference"),
                1
        );
        final FDBRecordContext context = directory.getCallerContext();
        assertThrows(RecordCoreArgumentException.class,
                () -> LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK, seekFuture, context));
    }

    @Test
    public void testWriteSeekData() throws Exception {
        directory.writeFDBLuceneFileReference("testReference1", new FDBLuceneFileReference(1, 1, 1, 1));
        final EmptyIndexInput emptyInput = new EmptyIndexInput("Empty Input");
        assertNull(directory.readBlock(emptyInput,
                "testReference1", directory.getFDBLuceneFileReferenceAsync("testReference1"), 1).get());
        directory.writeFDBLuceneFileReference("testReference2", new FDBLuceneFileReference(2, 1, 1, 200));
        byte[] data = "test string for write".getBytes();
        directory.writeData(2, 1, data);
        assertNotNull(directory.readBlock(emptyInput, "testReference2",
                directory.getFDBLuceneFileReferenceAsync("testReference2"), 1).get(), "seek data should exist");

        directory.getCallerContext().commit();
        assertCorrectMetricSize(LuceneEvents.SizeEvents.LUCENE_WRITE, 1, directory.getSerializer().encode(data).length);
    }

    @Test
    public void testListAll() throws IOException {
        assertEquals(directory.listAll().length, 0);
        directory.writeFDBLuceneFileReference("test1", new FDBLuceneFileReference(1, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test2", new FDBLuceneFileReference(2, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test3", new FDBLuceneFileReference(3, 1, 1, 1));
        assertArrayEquals(new String[]{"test1", "test2", "test3"}, directory.listAll());
        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_LIST_ALL, 2);
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
        assertThrows(NoSuchFileException.class, () -> directory.deleteFile("NonExist"));
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        directory.deleteFile("test1");
        assertEquals(directory.listAll().length, 0);

        // WAIT only gets called if there's a future to wait on, and so this value can be less than 2 if
        // the futures complete quickly
        assertMetricCountAtMost(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE, 2);
    }

    @Test
    public void testFileLength() throws Exception {
        long expectedSize = 20;
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, expectedSize, expectedSize, 1024);
        directory.writeFDBLuceneFileReference("test1", reference1);
        long fileSize = directory.fileLength("test1");
        assertEquals(expectedSize, fileSize);
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

    @SuppressWarnings("unused") // used to provide arguments for parameterized test
    static Stream<Arguments> testFileLengthNonExistent() {
        return Stream.of("nonExist", "nonExistentEntries.cfe", "nonExistentSegmentInfo.si")
                .map(Arguments::of);
    }

    @ParameterizedTest(name = "testFileLengthNonExistent[fileName={0}]")
    @MethodSource
    public void testFileLengthNonExistent(String fileName) {
        assertThrows(NoSuchFileException.class, () -> directory.fileLength(fileName));
    }

    @Test
    public void testRename() {
        final IOException ioException = assertThrows(IOException.class, () -> directory.rename("NoExist", "newName"));
        assertTrue(ioException.getCause() instanceof RecordCoreArgumentException);

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

    @Test
    void testDataBlocksProperlyCleared() throws Exception {
        // Create a file with data blocks
        long fileId = 1L;
        FDBLuceneFileReference reference = new FDBLuceneFileReference(fileId, 100, 100, 1024);
        String fileName = "test_file.dat";
        // Write file reference and data blocks
        directory.writeFDBLuceneFileReference(fileName, reference);
        // Write data to blocks
        byte[] testData1 = "test data block 1".getBytes();
        byte[] testData2 = "test data block 2".getBytes();
        directory.writeData(fileId, 1, testData1);
        directory.writeData(fileId, 2, testData2);
        
        // Verify data blocks exist
        final EmptyIndexInput emptyInput = new EmptyIndexInput("Test");
        final byte[] block1 = directory.readBlock(emptyInput, fileName, directory.getFDBLuceneFileReferenceAsync(fileName), 1).join();
        final byte[] block2 = directory.readBlock(emptyInput, fileName, directory.getFDBLuceneFileReferenceAsync(fileName), 2).join();
        Assertions.assertThat(block1).isNotEmpty();
        Assertions.assertThat(block2).isNotEmpty();

        // Delete the file
        directory.deleteFile(fileName);
        
        // Verify file is gone from directory listing
        assertEquals(0, directory.listAll().length);
        
        // Shortcut to ensure the index is empty: No file references, no data blocks
        final List<KeyValue> subspaceKeys = context.ensureActive().getRange(directory.getSubspace().range()).asList().join();
        Assertions.assertThat(subspaceKeys).isEmpty();
    }

    @Test
    void testNoBlockLeaksWithMultipleFiles() throws Exception {
        // Create multiple files with data blocks
        long fileId1 = 100L;
        long fileId2 = 200L;
        FDBLuceneFileReference ref1 = new FDBLuceneFileReference(fileId1, 50, 50, 512);
        FDBLuceneFileReference ref2 = new FDBLuceneFileReference(fileId2, 75, 75, 1024);
        String fileName1 = "file1.dat";
        String fileName2 = "file2.dat";
        // Write file references and data
        directory.writeFDBLuceneFileReference(fileName1, ref1);
        directory.writeFDBLuceneFileReference(fileName2, ref2);
        // Write data to blocks
        byte[] data1 = "data for file 1".getBytes();
        byte[] data2 = "data for file 2".getBytes();
        directory.writeData(fileId1, 1, data1);
        directory.writeData(fileId2, 1, data2);

        // There should be one key for the file reference and one for the data block
        final int keysPerFile = 2;
        // Get initial data subspace size
        final List<KeyValue> initialKeys = context.ensureActive().getRange(directory.getSubspace().range()).asList().join();
        Assertions.assertThat(initialKeys).hasSize(2 * keysPerFile);

        // Delete first file
        directory.deleteFile(fileName1);
        
        // Check that data subspace size reduced appropriately
        final List<KeyValue> afterDeleteKeys = context.ensureActive().getRange(directory.getSubspace().range()).asList().join();
        Assertions.assertThat(afterDeleteKeys).hasSize(1 * keysPerFile);
        
        // Delete second file
        directory.deleteFile(fileName2);
        
        // Check that data subspace is now empty
        final List<KeyValue> finalKeys = context.ensureActive().getRange(directory.getSubspace().range()).asList().join();
        Assertions.assertThat(finalKeys).isEmpty();
    }

    @Test
    void testQueueIndicatorLifecycle() {
        // 1. Initially, shouldUseQueue should return false
        assertFalse(directory.shouldUseQueue(),
                "shouldUseQueue should return false when no indicator is set");

        // 2. Set the queue indicator
        directory.setOngoingMergeIndicator();

        // 3. After setting, shouldUseQueue should return true in same transaction
        assertTrue(directory.shouldUseQueue(),
                "shouldUseQueue should return true after setUseQueue is called");

        // 4. Commit and verify persistence
        context.commit();

        // 5. Open new context and verify indicator persists
        try (FDBRecordContext newContext = fdb.openContext()) {
            FDBDirectory newDirectory = createDirectory(subspace, newContext, null);

            assertTrue(newDirectory.shouldUseQueue(),
                    "shouldUseQueue should return true in new transaction after commit");

            // 6. Attempt to clear with non-empty queue should fail
            // First, add an item to the queue
            PendingWriteQueue queue = newDirectory.createPendingWritesQueue();
            queue.enqueueInsert(newContext,
                    com.apple.foundationdb.tuple.Tuple.from("testDoc", 1),
                    createTestFields());
            newContext.commit();
        }

        // 7. Try to clear indicator with non-empty queue - should fail
        try (FDBRecordContext newContext = fdb.openContext()) {
            FDBDirectory newDirectory = createDirectory(subspace, newContext, null);

            com.apple.foundationdb.record.RecordCoreException exception =
                    assertThrows(com.apple.foundationdb.record.RecordCoreException.class,
                            newDirectory::clearOngoingMergeIndicatorButFailIfNonEmpty,
                            "clearUseQueueFailIfNonEmpty should throw when queue is not empty");

            assertThat(exception.getMessage(),
                    org.hamcrest.Matchers.containsString("pending write queue is not empty"));

            // Indicator should still be set
            assertTrue(newDirectory.shouldUseQueue(),
                    "shouldUseQueue should still return true after failed clear attempt");
        }

        // 8. Clear the queue
        try (FDBRecordContext newContext = fdb.openContext()) {
            FDBDirectory newDirectory = createDirectory(subspace, newContext, null);
            PendingWriteQueue queue = newDirectory.createPendingWritesQueue();

            // Read and clear all queue entries
            com.apple.foundationdb.record.RecordCursor<PendingWriteQueue.QueueEntry> cursor =
                    queue.getQueueCursor(newContext,
                            com.apple.foundationdb.record.ScanProperties.FORWARD_SCAN,
                            null);

            cursor.forEach(entry -> queue.clearEntry(newContext, entry)).join();
            newContext.commit();
        }

        // 9. Now clearing the indicator should succeed
        try (FDBRecordContext newContext = fdb.openContext()) {
            FDBDirectory newDirectory = createDirectory(subspace, newContext, null);

            // Should still be set before clear
            assertTrue(newDirectory.shouldUseQueue(),
                    "shouldUseQueue should return true before clear");

            // Clear should succeed now
            newDirectory.clearOngoingMergeIndicatorButFailIfNonEmpty();

            // Should be false immediately after clear in same transaction
            assertFalse(newDirectory.shouldUseQueue(),
                    "shouldUseQueue should return false after clearUseQueueFailIfNonEmpty");

            newContext.commit();
        }

        // 10. Verify indicator remains cleared after commit
        try (FDBRecordContext newContext = fdb.openContext()) {
            FDBDirectory newDirectory = createDirectory(subspace, newContext, null);

            assertFalse(newDirectory.shouldUseQueue(),
                    "shouldUseQueue should return false in new transaction after clear and commit");
        }
    }

    private java.util.List<com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord.DocumentField> createTestFields() {
        return java.util.List.of(
                new com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord.DocumentField(
                        "testField",
                        "test value",
                        com.apple.foundationdb.record.lucene.LuceneIndexExpressions.DocumentFieldType.TEXT,
                        false,
                        false,
                        java.util.Map.of()
                )
        );
    }
}
