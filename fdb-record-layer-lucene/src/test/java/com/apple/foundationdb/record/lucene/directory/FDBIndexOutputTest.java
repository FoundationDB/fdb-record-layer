/*
 * FDBIndexInputTest.java
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
import com.apple.test.Tags;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

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

    @Test
    public void testCopyBytesReadAheadPipelineShortBuffer() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.writeBytes(BLOCK_ARRAY_100, BLOCK_ARRAY_100.length);
        output.close();
        assertEquals(BLOCK_ARRAY_100.length, directory.getFDBLuceneFileReference(FILE_NAME).getSize());
        IndexInput blocks = directory.openChecksumInput(FILE_NAME, IOContext.READONCE);
        output = new FDBIndexOutput(FILE_NAME_TWO, directory);
        output.setExpectedBytes((PrefetchableBufferedChecksumIndexInput) blocks, 2 * FDBDirectory.DEFAULT_BLOCK_SIZE);
        assertEquals("[(1,0), (1,1)]", directoryCacheToString());
    }

    private String directoryCacheToString() {
        return directory.getBlockCache().asMap().keySet().stream().sorted().collect(Collectors.toList()).toString();
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
        assertEquals("[(1,0), (1,1), (1,2), (1,3), (1,4), (1,5), (1,6), (1,7), (1,8), (1,9)]",
                directoryCacheToString());
        byte[] fooey = new byte[FDBDirectory.DEFAULT_BLOCK_SIZE];
        blocks.readBytes(fooey, 0, fooey.length); // 1 block (refresh)
        assertEquals("[(1,0), (1,1), (1,2), (1,3), (1,4), (1,5), (1,6), (1,7), (1,8), (1,9), (1,10)]",
                directoryCacheToString());
        blocks.readBytes(fooey, 0, fooey.length); // next block, no fetch
        blocks.readBytes(fooey, 0, fooey.length); // another block, no fetch
        assertEquals("[(1,0), (1,1), (1,2), (1,3), (1,4), (1,5), (1,6), (1,7), (1,8), (1,9), (1,10), (1,11), " +
                     "(1,12)]",
                directoryCacheToString());
        fooey = new byte[10 * FDBDirectory.DEFAULT_BLOCK_SIZE];
        blocks.readBytes(fooey, 0, fooey.length); // another block, no fetch
        assertEquals("[(1,0), (1,1), (1,2), (1,3), (1,4), (1,5), (1,6), (1,7), (1,8), (1,9), (1,10), (1,11), " +
                     "(1,12), (1,13), (1,14), (1,15), (1,16), (1,17), (1,18), (1,19), (1,20), (1,21), (1,22)]",
                directoryCacheToString());
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
        List<Pair<Integer, Integer>> state = new ArrayList<>();
        state.add(Pair.of(-1, directory.getBlockCache().asMap().size()));
        for (int i = 0; i < 100; i++) {
            blocks.readBytes(fooey, 0, fooey.length);
            state.add(Pair.of(i, directory.getBlockCache().asMap().size()));
        }
        assertEquals("[(-1,10), (0,11), (1,12), (2,13), (3,14), (4,15), (5,16), (6,17), (7,18), (8,19), " +
                     "(9,20), (10,21), (11,22), (12,23), (13,24), (14,25), (15,26), (16,27), (17,28), (18,29), " +
                     "(19,30), (20,31), (21,32), (22,33), (23,34), (24,35), (25,36), (26,37), (27,38), (28,39), " +
                     "(29,40), (30,41), (31,42), (32,43), (33,44), (34,45), (35,46), (36,47), (37,48), (38,49), " +
                     "(39,50), (40,51), (41,52), (42,53), (43,54), (44,55), (45,56), (46,57), (47,58), (48,59), " +
                     "(49,60), (50,61), (51,62), (52,63), (53,64), (54,65), (55,66), (56,67), (57,68), (58,69), " +
                     "(59,70), (60,71), (61,72), (62,73), (63,74), (64,75), (65,76), (66,77), (67,78), (68,79), " +
                     "(69,80), (70,81), (71,82), (72,83), (73,84), (74,85), (75,86), (76,87), (77,88), (78,89), " +
                     "(79,90), (80,91), (81,92), (82,93), (83,94), (84,95), (85,96), (86,97), (87,98), (88,99), " +
                     "(89,100), (90,100), (91,100), (92,100), (93,100), (94,100), (95,100), (96,100), (97,100), " +
                     "(98,100), (99,101)]", state.toString());
        // The 101 is a bug on the read side where it is not checking that it does not need to read ahead to the last
        // block when the length is exhausted.

    }

    @Test
    void testCloseTwice() {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        output.close();
        assertDoesNotThrow(output::close);
    }
}
