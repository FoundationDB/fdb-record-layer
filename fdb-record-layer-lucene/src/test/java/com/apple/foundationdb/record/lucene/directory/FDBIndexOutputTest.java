/*
 * FDBIndexOutputTest.java
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

import com.apple.foundationdb.record.lucene.codec.PrefetchableBufferedChecksumIndexInput;
import com.apple.foundationdb.record.util.pair.ComparablePair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.Tags;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Test to FDBIndexOutput functionality.
 *
 */
@Tag(Tags.RequiresFDB)
public class FDBIndexOutputTest extends FDBDirectoryBaseTest {
    private static final String FILE_NAME = "y";
    private static final String FILE_NAME_TWO = "z";
    private static final Random RANDOM = new Random();
    private static final byte[] BLOCK_ARRAY_100 = new byte[100 * FDBDirectory.DEFAULT_BLOCK_SIZE];

    static {
        RANDOM.nextBytes(BLOCK_ARRAY_100);
    }

    @Test
    public void testWriteBytes() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        byte[] data = new byte[randomInt(256)];
        random.nextBytes(data);
        output.writeBytes(data, data.length);
        output.close();
        assertEquals(data.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
    }
    
    @Test
    public void testWriteByte() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeByte((byte) 0);
        output.close();
        assertEquals(1, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
    }

    @Test
    public void testCopyBytesPipeline() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.copyBytes(blocks, blocks.length());
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME_TWO).getSize());
    }

    @Nonnull
    private List<ComparablePair<Long, Integer>> directoryCacheKeys() {
        return directory.getBlockCache().asMap().keySet().stream().sorted().collect(Collectors.toList());
    }

    @Test
    public void testCopyBytesReadAheadPipelineShortBuffer() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, 2 * FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(List.of(Pair.of(1L, 0), Pair.of(1L, 1)), directoryCacheKeys());
    }

    @Test
    public void testCopyBytesReadAheadPipelineGreaterThanBuffer() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, 50 * FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(IntStream.range(0, 10).mapToObj(i -> Pair.of(1L, i)).collect(Collectors.toList()),
                directoryCacheKeys());
        byte[] fooey = new byte[FDBDirectory.DEFAULT_BLOCK_SIZE];
        blocks.readBytes(fooey, 0, fooey.length); // 1 block (refresh)
        assertEquals(IntStream.range(0, 11).mapToObj(i -> Pair.of(1L, i)).collect(Collectors.toList()),
                directoryCacheKeys());
        blocks.readBytes(fooey, 0, fooey.length); // next block, no fetch
        blocks.readBytes(fooey, 0, fooey.length); // another block, no fetch
        assertEquals(IntStream.range(0, 13).mapToObj(i -> Pair.of(1L, i)).collect(Collectors.toList()),
                directoryCacheKeys());
        fooey = new byte[10 * FDBDirectory.DEFAULT_BLOCK_SIZE];
        blocks.readBytes(fooey, 0, fooey.length); // another block, no fetch
        assertEquals(IntStream.range(0, 23).mapToObj(i -> Pair.of(1L, i)).collect(Collectors.toList()),
                directoryCacheKeys());
    }

    @Test
    public void testSeekWithinPrefetchDuringCopyBytes() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, 50 * FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(10, directory.getBlockCache().asMap().size());
        blocks.seek(FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(11, directory.getBlockCache().asMap().size());
    }

    @Test
    public void testReadsOverLimit() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, 50 * FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(10, directory.getBlockCache().asMap().size());
        blocks.seek(FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals(11, directory.getBlockCache().asMap().size());
    }

    @Test
    public void testSingleByteReadDoesNotAdvance() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, (long)(2.5 * 160 * 1024));
        assertEquals(10, directory.getBlockCache().asMap().size());
        blocks.readByte();
        assertEquals(10, directory.getBlockCache().asMap().size());
    }

    @Test
    public void testCycleThrough() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, (100L * FDBDirectory.DEFAULT_BLOCK_SIZE));
        byte[] fooey = new byte[FDBDirectory.DEFAULT_BLOCK_SIZE];
        Map<Integer, Integer> state = new HashMap<>();
        state.put(-1, directory.getBlockCache().asMap().size());
        for (int i = 0; i < 100; i++) {
            blocks.readBytes(fooey, 0, fooey.length);
            state.put(i, directory.getBlockCache().asMap().size());
        }
        // The 101 is a bug on the read side where it is not checking that it does not need to read ahead to the last
        // block when the length is exhausted.
        Map<Integer, Integer> expectedElements = IntStream.range(-1, 100)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), i -> i == 99 ? 101 : Math.min(i + 11, 100)));
        assertEquals(expectedElements, state);
    }

    @Test
    void testCloseTwice() throws IOException {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.close();
        assertDoesNotThrow(output::close);
    }
}
