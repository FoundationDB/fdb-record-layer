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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.test.Tags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
        directory.getContext().ensureActive().commit().join();
        FDBRecordContext context = fdb.openContext();
        directory = new FDBDirectory(subspace, context);
        assertEquals(3, directory.getIncrement());

        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_GET_INCREMENT_CALLS,2);
    }


    @Test
    public void testWriteGetLuceneFileReference() throws Exception {
        CompletableFuture<FDBLuceneFileReference> luceneFileReference = directory.getFDBLuceneFileReference("NonExist");
        assertNull(luceneFileReference.get(5, TimeUnit.SECONDS));
        String luceneReference1 = "luceneReference1";
        FDBLuceneFileReference fileReference = new FDBLuceneFileReference(1, 10, 10, 10);
        directory.writeFDBLuceneFileReference(luceneReference1, fileReference);
        luceneFileReference = directory.getFDBLuceneFileReference(luceneReference1);
        FDBLuceneFileReference actual = luceneFileReference.get(5, TimeUnit.SECONDS);
        assertNotNull(actual, "file reference should exist");
        assertEquals(actual, fileReference);

        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_GET_FILE_REFERENCE,1);
    }

    @Test
    public void testWriteLuceneFileReference() throws Exception {
        // write already created file reference
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(2, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(3, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference2);
        CompletableFuture<FDBLuceneFileReference> luceneFileReference = directory.getFDBLuceneFileReference("test1");
        assertNotNull(luceneFileReference.get(5, TimeUnit.SECONDS), "fileReference should exist");

        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_SIZE,
                LuceneSerializer.encode(reference1.getBytes(), true, false).length + LuceneSerializer.encode(reference2.getBytes(), true, false).length);
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_WRITE_FILE_REFERENCE_CALL, 2);
    }

    @Test
    public void testMissingSeek() {
        assertThrows(RecordCoreArgumentException.class, () -> directory.readBlock("testDescription", directory.getFDBLuceneFileReference("testReference"), 1));
    }

    @Test
    public void testWriteSeekData() throws Exception {
        directory.writeFDBLuceneFileReference("testReference1", new FDBLuceneFileReference(1, 1, 1, 1));
        assertNull(directory.readBlock("testReference1", directory.getFDBLuceneFileReference("testReference1"), 1).get());
        directory.writeFDBLuceneFileReference("testReference2", new FDBLuceneFileReference(2, 1, 1, 200));
        byte[] data = "test string for write".getBytes();
        directory.writeData(2, 1, data);
        assertNotNull(directory.readBlock("testReference2",
                directory.getFDBLuceneFileReference("testReference2"), 1).get(), "seek data should exist");

        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_WRITE_SIZE, LuceneSerializer.encode(data, true, false).length);
        assertCorrectMetricCount(LuceneEvents.Counts.LUCENE_WRITE_CALL, 1);
    }

    @Test
    public void testListAll() {
        assertEquals(directory.listAll().length, 0);
        directory.writeFDBLuceneFileReference("test1", new FDBLuceneFileReference(1, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test2", new FDBLuceneFileReference(2, 1, 1, 1));
        directory.writeFDBLuceneFileReference("test3", new FDBLuceneFileReference(3, 1, 1, 1));
        assertArrayEquals(new String[]{"test1", "test2", "test3"}, directory.listAll());
        directory.getContext().ensureActive().cancel();
        FDBRecordContext context = fdb.openContext();
        directory = new FDBDirectory(subspace, context);
        assertArrayEquals(new String[0], directory.listAll());

        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_LIST_ALL,2);
    }

    @Test
    public void testDeleteData() throws Exception {
        assertThrows(NoSuchFileException.class, () -> directory.deleteFile("NonExist"));
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        directory.deleteFile("test1");
        assertEquals(directory.listAll().length, 0);

        assertCorrectMetricCount(LuceneEvents.Waits.WAIT_LUCENE_DELETE_FILE,2);
    }

    @Test
    public void testFileLength() throws Exception {
        assertThrows(NoSuchFileException.class, () -> directory.fileLength("nonExist"));
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 20, 20, 1024);
        directory.writeFDBLuceneFileReference("test1", reference1);
        long fileSize = directory.fileLength("test1");
        assertEquals(20, fileSize);

        assertCorrectMetricCount(LuceneEvents.Events.LUCENE_GET_FILE_REFERENCE,1);
        assertCorrectMetricCount(LuceneEvents.Waits.WAIT_LUCENE_FILE_LENGTH,1);
    }

    @Test
    public void testRename() {
        assertThrows(RecordCoreArgumentException.class, () -> directory.rename("NoExist", "newName"));

        assertCorrectMetricCount(LuceneEvents.Waits.WAIT_LUCENE_RENAME,1);
    }


    private void assertCorrectMetricCount(StoreTimer.Event metric, int expectedValue) {
        //check that metrics were collected
        final Collection<StoreTimer.Event> events = timer.getEvents();
        Assertions.assertTrue(events.contains(metric),"Did not count get increment calls!");
        Assertions.assertEquals(expectedValue,timer.getCounter(metric).getCount(),"Incorrect call count ");
    }

}
