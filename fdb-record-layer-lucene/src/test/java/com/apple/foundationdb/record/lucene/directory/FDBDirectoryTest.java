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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void testGetIncrement() {
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
    public void testConcurrentGetIncrement() {
        final int threads = 50;
        List<CompletableFuture<Long>> futures = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.supplyAsync(directory::getIncrement, directory.getCallerContext().getExecutor()));
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

        assertCorrectMetricSize(LuceneEvents.SizeEvents.LUCENE_WRITE_FILE_REFERENCE, 2,
                LuceneSerializer.encode(reference1.getBytes(), true, false).length + LuceneSerializer.encode(reference2.getBytes(), true, false).length);
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
                () -> context.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK, seekFuture));
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
        assertCorrectMetricSize(LuceneEvents.SizeEvents.LUCENE_WRITE, 1, LuceneSerializer.encode(data, true, false).length);
    }

    @Test
    public void testListAll() {
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
        assertThrows(RecordCoreArgumentException.class, () -> directory.rename("NoExist", "newName"));

        assertCorrectMetricCount(LuceneEvents.Waits.WAIT_LUCENE_RENAME, 1);
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
}
